package reader

// buffer is a small {bytes, cursor} pair so Read can drain a prefetched
// chunk byte-by-byte without re-slicing on every call. Trimmed (leftCut /
// rightCut) buffers are stored as their tail/head slices; offset stays 0.
type buffer struct {
	buf    []byte
	offset int
	err    error // sticky producer error; surfaced when buf is exhausted
}

func (b *buffer) isEmpty() bool { return b == nil || b.offset >= len(b.buf) }

func (b *buffer) bytes() []byte { return b.buf[b.offset:] }

func (b *buffer) advance(n int) {
	b.offset += n
	if b.offset > len(b.buf) {
		b.offset = len(b.buf)
	}
}
