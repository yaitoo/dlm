package dlm

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/rpc"
)

// Start start the node and its RPC service
func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	l, err := net.Listen("tcp", n.addr)
	if err != nil {
		return err
	}

	_, err = n.db.ExecContext(context.Background(), CreateTableLease)
	if err != nil {
		return err
	}

	_, err = n.db.ExecContext(context.Background(), CreateTableTopic)
	if err != nil {
		return err
	}

	n.server = rpc.NewServer()
	n.listener = l
	go n.waitRequest()
	n.stopped = false
	n.logger.Info("dlm: node is running")
	return n.server.RegisterName("dlm", n)
}

// Stop stop the node and its RPC service
func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.listener.Close()
	n.stopped = true
	n.logger.Info("dlm: node is stopped")
}

func (n *Node) isStopped() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.stopped
}

func (n *Node) waitRequest() {

	for {
		conn, err := n.listener.Accept()

		if n.isStopped() {
			return
		}

		if err != nil {
			// listener is closed
			if errors.Is(err, net.ErrClosed) {
				return
			}

			n.logger.Warn("dlm: wait request", slog.String("err", err.Error()), slog.String("addr", n.addr))
			continue
		}

		go n.server.ServeConn(conn)
	}
}
