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

	return rpc.NewClient(&Conn{ctx: ctx, Conn: c}), nil
}

// Conn wrap net.Conn with context support
type Conn struct {
	ctx context.Context
	net.Conn
	once sync.Once
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
	go c.once.Do(c.waitContext)
	return c.Conn.Read(p)
}

func (c *Conn) Write(p []byte) (n int, err error) {
	return c.Conn.Write(p)
}
