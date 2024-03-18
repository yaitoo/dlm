package dlm

import (
	"io"
)

type NodeOption func(n *Node)

func WithRaft(id string, addr string) NodeOption {
	return func(n *Node) {
		n.raftID = id
		n.raftAddr = addr
	}
}

func WithDataDir(dir string) NodeOption {
	return func(n *Node) {
		n.dir = dir
	}
}

func WithLogger(w io.Writer) NodeOption {
	return func(n *Node) {
		n.logger = w
	}
}
