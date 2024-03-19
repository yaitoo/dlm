package dlm

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/rpc"
)

// Start start the node and its RPC service
func (n *Node) Start(ctx context.Context) error {
	l, err := net.Listen("tcp", n.addr)
	if err != nil {
		return err
	}

	_, err = n.db.ExecContext(ctx, CreateTableLease)
	if err != nil {
		return err
	}

	_, err = n.db.ExecContext(ctx, CreateTableTopic)
	if err != nil {
		return err
	}

	n.server = rpc.NewServer()
	go n.waitClose(ctx, l)
	go n.waitRequest(l)

	return n.server.RegisterName("dlm", n)
}

// Stop stop the node and its RPC service
func (n *Node) Stop() {
	go func() {
		n.close <- struct{}{}
	}()
	n.logger.Info("dlm: node stopped")
}

func (n *Node) waitRequest(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err == nil {
			go n.server.ServeConn(conn)
		} else {
			if errors.Is(err, net.ErrClosed) {
				return
			}

			n.logger.Warn("dlm: wait request", slog.String("err", err.Error()), slog.String("addr", n.addr))
		}

	}
}

func (n *Node) waitClose(ctx context.Context, l net.Listener) {

	select {
	case <-n.close:
	case <-ctx.Done():
	}

	l.Close()
}
