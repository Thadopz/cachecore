package cache

// ByteView is an immutable view of cached bytes.
type ByteView struct {
	b        []byte
	notFound bool
}

// Len returns the number of bytes in the view.
func (v ByteView) Len() int {
	return len(v.b)
}

func (v ByteView) String() string {
	return string(v.b)
}

// ByteSlice returns a copy of the view bytes.
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
