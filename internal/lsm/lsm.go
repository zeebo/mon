package lsm

import (
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/zeebo/errs"
	"github.com/zeebo/mon/internal/lsm/system"
	"golang.org/x/sys/unix"
)

// type file = *os.File

type file = system.File

func fileCreate(path string) (file, error) { return system.Create(path) }
func fileOpen(path string) (file, error)   { return system.Open(path) }
func fileRename(old, new string) error     { return system.Rename(old, new) }
func fileRemove(path string) error         { return system.Remove(path) }

type levelFiles struct {
	entries file
	values  file
	names   levelNames
}

type levelNames struct {
	entriesName    string
	entriesNameTmp string
	valuesName     string
	valuesNameTmp  string
}

type Options struct {
	MemCap    uint64
	NoWAL     bool
	NoWALSync bool
}

type T struct {
	mu     sync.Mutex
	dir    string
	ndir   string
	opts   Options
	wal    *wal
	mem    mem
	files  []*levelFiles
	names  []levelNames
	entBuf []byte
	valBuf []byte
}

func New(dir string, opts Options) (*T, error) {
	var t T
	return &t, initT(&t, dir, opts)
}

func initT(t *T, dir string, opts Options) (err error) {
	if opts.MemCap == 0 {
		opts.MemCap = 16 << 20
	}

	t.dir = dir
	t.ndir = dir + "\x00"
	t.opts = opts
	t.mem.init(opts.MemCap)
	t.entBuf = make([]byte, 0, bufferSize)
	t.valBuf = make([]byte, 0, bufferSize)

	walFh, err := fileCreate(filepath.Join(dir, "wal\x00"))
	if err != nil {
		return errs.Wrap(err)
	}
	defer func() {
		if err != nil {
			err = errs.Combine(err, walFh.Close())
		}
	}()

	iter := newWALIterator(walFh)
	for {
		_, key, value, ok := iter.Next()
		if !ok {
			break
		}
		t.mem.SetBytes(key, value)
	}
	if err := iter.Err(); err != nil {
		return errs.Wrap(err)
	}
	if read, pref := iter.Consumed(); pref {
		if err := walFh.Truncate(read); err != nil {
			return errs.Wrap(err)
		} else if err := walFh.Sync(); err != nil {
			return errs.Wrap(err)
		}
	}

	if !t.opts.NoWAL {
		t.wal = newWAL(walFh, !t.opts.NoWALSync)
	}

	// TODO: load up entries and values

	return nil
}

func (t *T) Close() (err error) {
	if t.wal != nil {
		err = errs.Combine(err, t.wal.Close())
	}
	for _, files := range t.files {
		if files != nil {
			err = errs.Combine(err, files.entries.Close())
			err = errs.Combine(err, files.values.Close())
		}
	}
	return err
}

func (t *T) SetBytes(key, value []byte) (err error) {
	return t.SetString(*(*string)(unsafe.Pointer(&key)), value)
}

func (t *T) SetString(key string, value []byte) (err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.wal != nil {
		if err := t.wal.AddString(key, value); err != nil {
			return err
		}
	}

	var now time.Time
	if trackStats {
		now = time.Now()
	}

	ok := t.mem.SetString(key, value)

	if trackStats {
		inserting += time.Since(now)
	}

	if ok {
		return nil
	}

	return t.snapshotCompact()
}

func (t *T) CompactAndSync() (err error) {
	if t.mem.Len() == 0 {
		return nil
	}
	return t.snapshotCompact()
}

func (t *T) snapshotCompact() (err error) {
	if trackStats {
		atomic.AddInt64(&snapshots, 1)
	}

	level := len(t.files)
	iters := t.mem.iters()

	for i, lf := range t.files {
		if lf == nil {
			level = i
			break
		}
		if _, err := lf.entries.Seek(0, io.SeekStart); err != nil {
			return errs.Wrap(err)
		}
		if err := unix.Fadvise(int(lf.entries.Fd()), 0, 0, unix.FADV_SEQUENTIAL); err != nil {
			return errs.Wrap(err)
		}
		if _, err := lf.values.Seek(0, io.SeekStart); err != nil {
			return errs.Wrap(err)
		}
		if err := unix.Fadvise(int(lf.values.Fd()), 0, 0, unix.FADV_SEQUENTIAL); err != nil {
			return errs.Wrap(err)
		}
		iters = append(iters, newFileIterator(lf.entries, lf.values))
	}

	for len(t.names) <= level {
		t.names = append(t.names, levelNames{
			entriesName:    filepath.Join(t.dir, fmt.Sprintf("%04d-entries.sst\x00", len(t.names))),
			entriesNameTmp: filepath.Join(t.dir, fmt.Sprintf("%04d-entries.sst.tmp\x00", len(t.names))),
			valuesName:     filepath.Join(t.dir, fmt.Sprintf("%04d-values.dat\x00", len(t.names))),
			valuesNameTmp:  filepath.Join(t.dir, fmt.Sprintf("%04d-values.dat.tmp\x00", len(t.names))),
		})
	}

	names := t.names[level]
	mg := newMerger(iters)

	entriesFh, err := fileCreate(names.entriesNameTmp)
	if err != nil {
		return errs.Wrap(err)
	}
	defer func() {
		if err != nil {
			err = errs.Combine(err, entriesFh.Close())
			err = errs.Combine(err, fileRemove(names.entriesNameTmp))
		}
	}()

	valuesFh, err := fileCreate(names.valuesNameTmp)
	if err != nil {
		return errs.Wrap(err)
	}
	defer func() {
		if err != nil {
			err = errs.Combine(err, valuesFh.Close())
			err = errs.Combine(err, fileRemove(names.valuesNameTmp))
		}
	}()

	entries := newWriteHandleBuf(entriesFh, t.entBuf)
	values := newWriteHandleBuf(valuesFh, t.valBuf)

	if err := writeFile(mg, entries, values); err != nil {
		return errs.Wrap(err)
	}

	if err := fileRename(names.valuesNameTmp, names.valuesName); err != nil {
		return errs.Wrap(err)
	}
	if err := t.syncDir(); err != nil {
		return errs.Wrap(err)
	}

	if err := fileRename(names.entriesNameTmp, names.entriesName); err != nil {
		return errs.Wrap(err)
	}
	if err := t.syncDir(); err != nil {
		return errs.Wrap(err)
	}

	newLevel := &levelFiles{
		entries: entriesFh,
		values:  valuesFh,
		names:   names,
	}

	if level >= len(t.files) {
		t.files = append(t.files, newLevel)
	} else {
		t.files[level] = newLevel
	}

	for i, lf := range t.files {
		if i >= level {
			break
		}

		if err := lf.entries.Close(); err != nil {
			return errs.Wrap(err)
		}
		if err := fileRemove(lf.names.entriesName); err != nil {
			return errs.Wrap(err)
		}
		if err := lf.values.Close(); err != nil {
			return errs.Wrap(err)
		}
		if err := fileRemove(lf.names.valuesName); err != nil {
			return errs.Wrap(err)
		}

		t.files[i] = nil
	}

	if t.wal != nil {
		if err := t.wal.Truncate(); err != nil {
			return errs.Wrap(err)
		}
	}

	if err := t.syncDir(); err != nil {
		return errs.Wrap(err)
	}

	t.mem.reset()
	return nil
}

func (t *T) syncDir() error {
	dir, err := fileOpen(t.ndir)
	if err != nil {
		return errs.Wrap(err)
	}
	err = errs.Combine(err, dir.Sync())
	err = errs.Combine(err, dir.Close())
	return errs.Wrap(err)
}
