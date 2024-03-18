package dlm

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

func (n *Node) NewLock(req LockRequest, t *Lease) error {
	if !n.isLeader() {
		return ErrNotRaftLeader
	}

	if strings.HasPrefix(req.Topic, "@") { // @ is reversed for LeaseTerms setting
		return ErrInvalidTopic
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	ttl, err := n.getTerms(req.Topic)

	if err != nil {
		return err
	}

	dbKey := req.Topic + ":" + req.Key
	lease, err := n.getLease(dbKey)

	if err == nil { // exits a lease
		if lease.Lessee != req.ID && lease.IsLive() { //someone else's lease is still live
			return ErrAlreadyLocked
		}
	} else {
		if !errors.Is(err, ErrNoLease) {
			return err
		}
	}

	// assign/reassign lease to current request
	lease = Lease{
		Lessee: req.ID,
		Topic:  req.Topic,
		Key:    req.Key,
		TTL:    ttl,
		Since:  time.Now().Unix(),
		Nonce:  n.raft.CommitIndex(),
	}

	err = n.applyCmdLease(CmdSet, dbKey, lease)
	if err != nil {
		return err
	}

	if t != nil {
		*t = lease
	}

	return nil
}

func (n *Node) RenewLock(req LockRequest, t *Lease) error {
	if !n.isLeader() {
		return ErrNotRaftLeader
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	ttl, err := n.getTerms(req.Topic)

	if err != nil {
		return err
	}

	dbKey := req.Topic + ":" + req.Key
	lease, err := n.getLease(dbKey)

	if err != nil {
		return err
	}

	if lease.Lessee != req.ID {
		return ErrNotYourLease
	}

	if !lease.IsLive() {
		return ErrExpiredLease
	}

	lease.Since = time.Now().Unix()
	lease.TTL = ttl

	err = n.applyCmdLease(CmdSet, dbKey, lease)
	if err != nil {
		return err
	}
	if t != nil {
		*t = lease
	}
	return nil
}

func (n *Node) ReleaseLock(req LockRequest, ok *bool) error {
	if !n.isLeader() {
		return ErrNotRaftLeader
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	dbKey := req.Topic + ":" + req.Key

	lease, err := n.getLease(dbKey)
	if err != nil {
		return err
	}

	if lease.Lessee != req.ID {
		return ErrNotYourLease
	}

	err = n.applyCmdRemove(dbKey)

	if err != nil {
		return err
	}

	*ok = true

	return nil
}

func (n *Node) SetTerms(req TermsRequest, ok *bool) error {
	if !n.isLeader() {
		return ErrNotRaftLeader
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.applyCmdTerms(CmdSet, TopicTerms+req.Topic, req.TTL)
}

func (n *Node) RemoveTerms(req TermsRequest, ok *bool) error {
	if !n.isLeader() {
		return ErrNotRaftLeader
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	err := n.applyCmdRemove(TopicTerms + req.Topic)

	if err != nil {
		return err
	}

	*ok = true
	return nil
}

// Join joins a new node, identified by id and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (n *Node) Join(p Peer, ok *bool) error {
	if !n.isLeader() {
		return ErrNotRaftLeader
	}

	nodeID := raft.ServerID(p.RaftID)
	addr := raft.ServerAddress(p.RaftAddr)

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == nodeID || srv.Address == addr {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == addr && srv.ID == nodeID {
				Logger.Info("dlm: node %s at %s already member of cluster, ignoring join request", p.RaftID, p.RaftAddr)
				*ok = true
				return nil
			}

			future := n.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := n.raft.AddVoter(nodeID, addr, 0, 0)
	if f.Error() != nil {
		return f.Error()
	}

	*ok = true
	return nil
}
