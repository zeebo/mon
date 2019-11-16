package lsm

import "io"

func writeFile(mg *merger, entries, values *writeHandle) error {
	forceWrite := len(mg.iters) == 1
	skipWrite := len(mg.iters) - 1
	var valueBuf []byte

	for {
		ele, r, err := mg.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if forceWrite || ele.idx != skipWrite {
			if kptr := ele.ent.Key(); kptr.Pointer() {
				kptr.SetOffset(values.Offset())
				key := ele.mkey
				if key == nil {
					key = ele.ent.Key().InlineData()
				}
				if err := values.Append(key); err != nil {
					return err
				}
			}
			if vptr := ele.ent.Value(); vptr.Pointer() {
				// TODO: ReadPointer always allocates from a file iterator.
				// we're just about to copy it into the values writer, so
				// we should be able to stream it directly out or something.

				var value []byte
				if vptr.Pointer() {
					value, err = r.AppendPointer(*vptr, valueBuf[:0])
					if err != nil {
						return err
					}
					valueBuf = value[:0]
				} else if vptr.Inline() {
					value = vptr.InlineData()
				}

				vptr.SetOffset(values.Offset())
				if err := values.Append(value); err != nil {
					return err
				}
			}
		}

		if err := entries.Append(ele.ent[:]); err != nil {
			return err
		}
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
