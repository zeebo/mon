package skipmem

const chunkSize = 200

type chunk struct {
	len  uint
	data [chunkSize]skipMemEntry
}

func (c *chunk) split() *chunk {
	c.len /= 2
	out := new(chunk)
	out.len = uint(copy(out.data[:], c.data[c.len:]))
	return out
}

func (c *chunk) insert(idx uint, val skipMemEntry) {
	copy(c.data[idx+1:c.len+1], c.data[idx:c.len])
	c.data[idx] = skipMemEntry(val)
	c.len++
}

func (c *chunk) full() bool { return c.len == chunkSize }

// TODO(jeff): could make this a linked list for lock-free inserts maybe.
// inserting into the chunk may be difficult, though...

type chunkCursor struct {
	cl    *chunkList
	chunk *chunk
	cidx  uint
	eidx  uint
}

type chunkList struct {
	chunks []*chunk
}

func (cl *chunkList) cursor() chunkCursor {
	if len(cl.chunks) == 0 {
		cl.chunks = append(cl.chunks, new(chunk))
	}
	return chunkCursor{
		cl:    cl,
		chunk: cl.chunks[0],
		cidx:  0,
		eidx:  0,
	}
}

func (cur chunkCursor) insert(val skipMemEntry) chunkCursor {
	if cur.chunk.full() {
		split := cur.chunk.split()
		cur.cl.chunks = append(cur.cl.chunks[:cur.cidx+1], cur.cl.chunks[cur.cidx:]...)
		cur.cl.chunks[cur.cidx+1] = split

		if cur.eidx > cur.chunk.len {
			cur.eidx -= cur.chunk.len
			cur.cidx++
			cur.chunk = split
		}
	}

	cur.chunk.insert(cur.eidx, val)
	cur.eidx++
	return cur
}

func (cur chunkCursor) next() (chunkCursor, bool) {
	cur.eidx++
	if cur.eidx > cur.chunk.len || (cur.eidx == cur.chunk.len && cur.cidx != uint(len(cur.cl.chunks))-1) {
		cur.eidx = 0
		cur.cidx++
		if cur.cidx >= uint(len(cur.cl.chunks)) {
			return chunkCursor{}, false
		}
		cur.chunk = cur.cl.chunks[cur.cidx]
	}
	return cur, true
}

func (cur chunkCursor) get() (skipMemEntry, bool) {
	if cur.eidx >= cur.chunk.len {
		return skipMemEntry{}, false
	}
	return cur.chunk.data[cur.eidx], true
}
