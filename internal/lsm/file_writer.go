package lsm

import "time"

func writeFile(mi iterator, entries, values *writeHandle) error {
	var now time.Time
	if trackStats {
		now = time.Now()
	}

	for mi.Next() {
		ent := mi.Entry()
		if err := entries.Append(ent[:]); err != nil {
			return err
		}
		if ent.Key().Pointer() {
			if err := values.Append(mi.Key()); err != nil {
				return err
			}
		}
		if ent.Value().Pointer() {
			if err := values.Append(mi.Value()); err != nil {
				return err
			}
		}
	}
	if err := mi.Err(); err != nil {
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

	if trackStats {
		writing += time.Since(now)
	}
	return nil
}
