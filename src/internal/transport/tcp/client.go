package tcp

import (
	"context"
	"net"
)

// Dial opens a single TCP connection to addr, respecting the context deadline.
// The caller is responsible for closing the returned conn.
//
// Usage: one-shot request/response — dial, send, receive, close.
// For long-lived connections or pooling, build on top of this.
func Dial(ctx context.Context, addr string) (net.Conn, error) {
	d := net.Dialer{}
	return d.DialContext(ctx, "tcp", addr)
}
