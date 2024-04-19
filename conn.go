package dlm

import (
	"context"
	"errors"
	"net"
	"net/rpc"
	"sync"
	"time"
)

func connect(ctx context.Context, addr string, timeout time.Duration) (*rpc.Client, error) {
	var d = net.Dialer{
		Timeout: timeout,
	}

	c, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	conn := &Conn{
		Conn: c,
	}

	conn.ctx, conn.cancel = context.WithCancel(ctx)

	go conn.waitContext()

	return rpc.NewClient(conn), nil
}

// Conn wrap net.Conn with context support
type Conn struct {
	ctx    context.Context
	cancel context.CancelFunc
	net.Conn
}

func (c *Conn) waitContext() {
	// disabled deadline
	c.Conn.SetDeadline(time.Time{}) // nolint: errcheck
	<-c.ctx.Done()
	err := c.ctx.Err()
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		c.Conn.SetDeadline(time.Unix(1, 0)) // nolint: errcheck
	}
}

func (c *Conn) Read(p []byte) (n int, err error) {
	return c.Conn.Read(p)
}

func (c *Conn) Write(p []byte) (n int, err error) {
	return c.Conn.Write(p)
}

func (c *Conn) Close() error {
	defer c.cancel()
	return c.Conn.Close()
}
