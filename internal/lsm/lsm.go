package lsm

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/zeebo/errs"
)

type levelFiles struct {
	entries     *os.File
	entriesName string
	values      *os.File
	valuesName  string
}

type Options struct {
	MemCap    uint64
	NoWAL     bool
	NoWALSync bool
}

var (
	inserting time.Duration
	writing   time.Duration
)

// type mem = btreeMem

type mem = heapMem

type T struct {
	dir   string
	opts  Options
	wal   *wal
	mem   *mem
	files []*levelFiles
}

func New(dir string, opts Options) (*T, error) {
	var t T
	return &t, initT(&t, dir, opts)
}

func initT(t *T, dir string, opts Options) (err error) {
	if opts.MemCap == 0 {
		opts.MemCap = 1 << 20
	}

	t.dir = dir
	t.opts = opts
	t.mem = (*mem).newMem(nil, opts.MemCap)

	var cl cleaner
	defer cl.Close(&err)

	walFh, err := os.OpenFile(filepath.Join(dir, "wal"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return errs.Wrap(err)
	}
	defer cl.Add(walFh.Close)

	iter := newWALIterator(walFh)
	for {
		_, key, value, err := iter.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return errs.Wrap(err)
		}
		t.mem.SetBytes(key, value)
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

func (t *T) SetString(key string, value []byte) (err error) {
	if t.wal != nil {
		if err := t.wal.AddString(key, value); err != nil {
			return err
		}
	}
	if t.mem.SetString(key, value) {
		return nil
	}
	return t.snapshotCompact()
}

func (t *T) SetBytes(key, value []byte) (err error) {
	if t.wal != nil {
		if err := t.wal.AddBytes(key, value); err != nil {
			return err
		}
	}
	now := time.Now()
	ok := t.mem.SetBytes(key, value)
	inserting += time.Since(now)
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
	level := len(t.files)
	iter := t.mem.iter()
	iters := []mergeIter{&iter}
	for i, lf := range t.files {
		if lf == nil {
			level = i
			break
		}

		fi, err := newFileIterator(lf.entries, lf.values)
		if err != nil {
			return errs.Wrap(err)
		}

		bi := newBatchMergeIterAdapter(fi, 4096/entrySize)
		iters = append(iters, bi)
	}

	mg, err := newMerger(iters)
	if err != nil {
		return errs.Wrap(err)
	}

	entriesName := filepath.Join(t.dir, fmt.Sprintf("%04d-entries.sst", level))
	valuesName := filepath.Join(t.dir, fmt.Sprintf("%04d-values.dat", level))

	var cl cleaner
	defer cl.Close(&err)

	entriesFh, err := os.Create(entriesName + ".tmp")
	if err != nil {
		return errs.Wrap(err)
	}
	cl.Add(func() (err error) {
		err = errs.Combine(err, entriesFh.Close())
		err = errs.Combine(err, os.Remove(entriesFh.Name()))
		return errs.Wrap(err)
	})
	entries, err := newWriteHandle(entriesFh, 4096)
	if err != nil {
		return errs.Wrap(err)
	}

	var valuesFh *os.File
	var valuesNameOld string
	var now time.Time

	if level == 0 {
		valuesFh, err = os.Create(valuesName + ".tmp")
		if err != nil {
			return errs.Wrap(err)
		}
		cl.Add(func() (err error) {
			err = errs.Combine(err, valuesFh.Close())
			err = errs.Combine(err, os.Remove(valuesFh.Name()))
			return errs.Wrap(err)
		})
		valuesNameOld = valuesFh.Name()
		now = time.Now()
	} else {
		valuesFh = t.files[level-1].values
		if _, err := valuesFh.Seek(0, io.SeekEnd); err != nil {
			return errs.Wrap(err)
		}
		valuesNameOld = t.files[level-1].valuesName
	}
	values, err := newWriteHandle(valuesFh, 4096)
	if err != nil {
		return errs.Wrap(err)
	}

	// fmt.Print("level ", level, " ")

	if err := writeFile(mg, entries, values); err != nil {
		return errs.Wrap(err)
	}

	if level == 0 {
		dur := time.Since(now)
		writing += dur
		// fmt.Println("dur ", dur)
	} else {
		// fmt.Println()
	}

	if err := os.Rename(entriesFh.Name(), entriesName); err != nil {
		return errs.Wrap(err)
	}
	if err := t.syncDir(); err != nil {
		return errs.Wrap(err)
	}

	if err := os.Rename(valuesNameOld, valuesName); err != nil {
		return errs.Wrap(err)
	}
	if err := t.syncDir(); err != nil {
		return errs.Wrap(err)
	}

	newLevel := &levelFiles{
		entries:     entriesFh,
		entriesName: entriesName,
		values:      valuesFh,
		valuesName:  valuesName,
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
		if err := os.Remove(lf.entriesName); err != nil {
			return errs.Wrap(err)
		}

		if lf.values != newLevel.values {
			if err := t.files[i].values.Close(); err != nil {
				return errs.Wrap(err)
			}
			if err := os.Remove(lf.valuesName); err != nil {
				return errs.Wrap(err)
			}
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
	dir, err := os.Open(t.dir)
	if err != nil {
		return errs.Wrap(err)
	}
	err = errs.Combine(err, dir.Sync())
	err = errs.Combine(err, dir.Close())
	return errs.Wrap(err)
}
