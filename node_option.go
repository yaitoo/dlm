package dlm

import (
	"log/slog"
)

type NodeOption func(n *Node)

func WithLogger(l *slog.Logger) NodeOption {
	return func(n *Node) {
		n.logger = l
	}
}
