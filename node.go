package dlm

import (
	"log/slog"
	"net"
	"net/rpc"
	"sync"

	"github.com/yaitoo/sqle"
)

func NewNode(addr string, db *sqle.DB, options ...NodeOption) *Node {
	n := &Node{
		addr:   addr,
		db:     db,
		frozen: make(map[string]struct{}),
		logger: slog.Default(),
	}

	for _, o := range options {
		o(n)
	}

	return n
}

type Node struct {
	mu     sync.RWMutex
	db     *sqle.DB
	logger *slog.Logger
	frozen map[string]struct{}

	stopped bool

	addr     string
	listener net.Listener
	server   *rpc.Server
}
