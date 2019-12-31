package layermem

import "testing"

func TestLayer(t *testing.T) {
	l := &layer{
		data: [fanout][]layerEntry{
			{{1, 0}, {3, 0}, {5, 0}},
			{{2, 0}, {4, 0}, {6, 0}},
			{{8, 0}, {9, 0}, {10, 0}},
			{{11, 0}, {12, 0}, {13, 0}},
			// {{14, 0}},
			// {{15, 0}},
			// {{16, 0}},
			// {{17, 0}},
		},
	}

	t.Log(l.merge(nil, nil))
}
