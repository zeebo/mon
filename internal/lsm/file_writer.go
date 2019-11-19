package lsm

import (
	"io"
)

func writeFile(mg *merger, entries, values *writeHandle) error {
	forceWrite := len(mg.iters) == 1
	skipWrite := len(mg.iters) - 1
	var valueBuf []byte

	entriesCtr, valuesCtr := 0, 0

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
				valuesCtr++
			}
			if vptr := ele.ent.Value(); vptr.Pointer() {
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
				valuesCtr++
			}
		}

		if err := entries.Append(ele.ent[:]); err != nil {
			return err
		}
		entriesCtr++
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

	// fmt.Print("entries ", entriesCtr, " values ", valuesCtr, " ")

	return nil
}
