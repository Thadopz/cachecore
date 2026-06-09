package cache

type ByteView struct {
	b        []byte
	notFound bool
}

func (v ByteView) Len() int {
	return len(v.b)
}

func (v ByteView) String() string {
	return string(v.b)
}

func (v ByteView) ByteSlice() []byte {
	return cloneBytes(v.b)
}

func (v ByteView) withNotFound() ByteView {
	v.notFound = true
	return v
}

func (v ByteView) isNotFound() bool {
	return v.notFound
}

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
