package skipmem

type chunkEntry struct {
	prefix uint64
	idx    uint32
}

const chunkSize = 4

type chunk struct {
	len   uint
	right *chunk
	down  *chunk
	data  [chunkSize]chunkEntry
}

func (c *chunk) reset() {
	c.len = 0
	c.right = nil
	c.down = nil
}

func (c *chunk) split() *chunk {
	c.len /= 2
	out := new(chunk)
	out.len = uint(copy(out.data[:], c.data[c.len:]))
	out.right = c.right

	// TODO: we can maybe advance down to the right here if we want to do
	// comparisons against the rightmost key.
	out.down = c.down

	c.right = out
	return out
}

func (c *chunk) insert(idx uint, val chunkEntry) {
	if idx >= c.len {
		c.data[c.len] = val
	} else {
		copy(c.data[idx+1:c.len+1], c.data[idx:c.len])
		c.data[idx] = val
	}
	c.len++
}

func (c *chunk) set(idx uint, val chunkEntry) {
	c.data[idx] = val
}

func (c *chunk) cursor() chunkCursor {
	return chunkCursor{chunk: c}
}

type chunkCursor struct {
	chunk *chunk
	idx   uint
}

func (cur *chunkCursor) insert(val chunkEntry) (didSplit bool) {
	if cur.chunk.len == chunkSize {
		didSplit = true
		split := cur.chunk.split()
		if cur.idx > cur.chunk.len {
			cur.idx -= cur.chunk.len
			cur.chunk = split
		}
	}

	cur.chunk.insert(cur.idx, val)
	return didSplit
}

func (cur *chunkCursor) set(val chunkEntry) {
	cur.chunk.set(cur.idx, val)
}

func (cur *chunkCursor) right() bool {
	cur.idx++

	if cur.idx >= cur.chunk.len {
		if cur.chunk.right == nil {
			return false
		}

		cur.idx = 0
		cur.chunk = cur.chunk.right
	}

	return true
}

func (cur *chunkCursor) down() {
	cur.idx = 0
	cur.chunk = cur.chunk.down
}

func (cur *chunkCursor) get() chunkEntry {
	return cur.chunk.data[cur.idx%chunkSize]
}

func (cur *chunkCursor) getPrefix() uint64 { return cur.chunk.data[cur.idx%chunkSize].prefix }
func (cur *chunkCursor) getIdx() uint32    { return cur.chunk.data[cur.idx%chunkSize].idx }
