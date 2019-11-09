package inlbytes

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestInlbytes(t *testing.T) {
	var buf []byte

	{
		inl := FromBytes(buf)
		assert.Equal(t, inl.Length(), 0)
		assert.Nil(t, inl.Bytes())
		assert.Equal(t, inl.String(), string(buf))
	}

	buf = []byte{}

	{
		inl := FromBytes(buf)
		assert.Equal(t, inl.Length(), 0)
		assert.DeepEqual(t, inl.Bytes(), buf)
		assert.Equal(t, inl.String(), string(buf))
	}

	{
		inl := FromString(string(buf))
		assert.Equal(t, inl.Length(), 0)
		assert.DeepEqual(t, inl.Bytes(), buf)
		assert.Equal(t, inl.String(), string(buf))
	}

	var val uint64
	for i := 0; i < 32; i++ {
		buf = append(buf, byte(i+1))
		if i < 7 {
			val |= uint64(i+1) << (48 - (8 * i))
		}

		{
			inl := FromBytes(buf)
			t.Logf("%014x %02x %02x", inl.Uint56(), inl.Rem, buf)
			assert.Equal(t, inl.Length(), i+1)
			assert.DeepEqual(t, inl.Bytes(), buf)
			assert.Equal(t, inl.String(), string(buf))
			assert.Equal(t, inl.Uint56(), val)
		}

		{
			inl := FromString(string(buf))
			assert.Equal(t, inl.Length(), i+1)
			assert.DeepEqual(t, inl.Bytes(), buf)
			assert.Equal(t, inl.String(), string(buf))
			assert.Equal(t, inl.Uint56(), val)
		}
	}
}
