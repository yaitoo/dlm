package dlm

import (
	"encoding/json"
	"io"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

func NewNode(addr string, options ...NodeOption) *Node {
	n := &Node{
		addr: addr,
		m:    make(map[string][]byte),
	}

	n.logger = os.Stderr

	for _, o := range options {
		o(n)
	}

	if n.raftID == "" {
		n.raftID = n.raftAddr
	}

	return n
}

type Node struct {
	mu sync.Mutex

	raftID   string
	raftAddr string

	dir    string
	logger io.Writer

	settings Settings

	addr   string
	server *rpc.Server

	m    map[string][]byte // The key-value store for the system.
	raft *raft.Raft        // The consensus mechanism
}

func (n *Node) isLeader() bool {
	return n.raft.State() == raft.Leader
}

func (n *Node) getLease(dbKey string) (Lease, error) {
	var l Lease
	buf, ok := n.m[dbKey]
	if !ok {
		return l, ErrNoLease
	}

	err := json.Unmarshal(buf, &l)
	if err != nil {
		return l, err
	}
	return l, nil
}

func (n *Node) getTerms(topic string) (time.Duration, error) {
	dbKey := TopicTerms + topic
	var ttl time.Duration

	buf, ok := n.m[dbKey]
	if ok {
		err := json.Unmarshal(buf, &ttl)
		if err != nil {
			return 0, err
		}
	} else {
		ttl = DefaultLeaseTerm
	}

	if ttl < 0 {
		return 0, ErrFrozenTopic
	}

	return ttl, nil
}

func (n *Node) applyCmdLease(name, dbKey string, l Lease) error {
	buf, err := json.Marshal(l)
	if err != nil {
		return err
	}
	cmd := cmd{
		Name:  name,
		Key:   dbKey,
		Value: buf,
	}
	buf, _ = json.Marshal(cmd)

	f := n.raft.Apply(buf, DefaultTimeout)

	return f.Error()
}

func (n *Node) applyCmdTerms(name, dbKey string, ttl time.Duration) error {
	buf, err := json.Marshal(ttl)
	if err != nil {
		return err
	}
	cmd := cmd{
		Name:  name,
		Key:   dbKey,
		Value: buf,
	}
	buf, _ = json.Marshal(cmd)

	f := n.raft.Apply(buf, DefaultTimeout)

	return f.Error()
}

func (n *Node) applyCmdRemove(dbKey string) error {
	cmd := cmd{
		Name: CmdRemove,
		Key:  dbKey,
	}
	buf, _ := json.Marshal(cmd)

	f := n.raft.Apply(buf, DefaultTimeout)

	return f.Error()
}
