package dlm

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
)

// Apply applies a Raft log entry to the key-value store.
func (n *Node) Apply(l *raft.Log) interface{} {
	if !n.isLeader() {
		n.mu.Lock()
		defer n.mu.Unlock()
	}

	var c cmd
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Name {
	case CmdSet:
		return n.applySet(c.Key, c.Value)
	case CmdRemove:
		return n.applyDelete(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command name: %s", c.Name))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (n *Node) Snapshot() (raft.FSMSnapshot, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Clone the map.
	o := make(map[string][]byte)
	for k, v := range n.m {
		o[k] = v
	}
	return &snapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (n *Node) Restore(rc io.ReadCloser) error {
	o := make(map[string][]byte)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	n.m = o
	return nil
}

func (n *Node) applySet(key string, value []byte) interface{} {
	n.m[key] = value
	return nil
}

func (n *Node) applyDelete(key string) interface{} {
	delete(n.m, key)
	return nil
}
