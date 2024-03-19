package dlm

import (
	"log/slog"
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
		close:  make(chan struct{}),
	}

	for _, o := range options {
		o(n)
	}

	return n
}

type Node struct {
	mu     sync.Mutex
	db     *sqle.DB
	logger *slog.Logger
	frozen map[string]struct{}

	addr   string
	server *rpc.Server
	close  chan struct{}
}
