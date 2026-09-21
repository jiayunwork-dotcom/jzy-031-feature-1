package engine

import "crypto/rand"

// newNonce returns a unique member id used by the sliding-log ZSET.
func newNonce() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	const hexd = "0123456789abcdef"
	out := make([]byte, 32)
	for i, x := range b {
		out[i*2] = hexd[x>>4]
		out[i*2+1] = hexd[x&0x0f]
	}
	return string(out)
}
