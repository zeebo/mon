package filewriter

import (
	"github.com/zeebo/mon/internal/lsm/file/writehandle"
	"github.com/zeebo/mon/internal/lsm/iterator"
)

func Write(i iterator.T, entries, values *writehandle.T) error {
	for i.Next() {
		ent := i.Entry()
		if err := entries.Append(ent[:]); err != nil {
			return err
		}
		if ent.Key().Pointer() {
			if err := values.Append(i.Key()); err != nil {
				return err
			}
		}
		if ent.Value().Pointer() {
			if err := values.Append(i.Value()); err != nil {
				return err
			}
		}
	}
	if err := i.Err(); err != nil {
		return err
	}

	if err := entries.Flush(); err != nil {
		return err
	} else if err := entries.Sync(); err != nil {
		return err
	} else if err := values.Flush(); err != nil {
		return err
	} else if err := values.Sync(); err != nil {
		return err
	}
	return nil
}
