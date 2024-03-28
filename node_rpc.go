package dlm

import (
	"errors"
	"log"
	"time"

	"github.com/yaitoo/sqle"
)

func (n *Node) NewLock(req LockRequest, t *Lease) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	_, ok := n.frozen[req.Topic]

	if ok {
		return ErrFrozenTopic
	}

	lease, err := n.getLease(req.Topic, req.Key)

	if err == nil { // exits a lease
		if lease.Lessee != req.ID && lease.IsLive() { // someone else's lease is still live
			return ErrLeaseExists
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
		TTL:    sqle.Duration(req.TTL),
		Since:  time.Now().Unix(),
	}
	if errors.Is(err, ErrNoLease) {
		err = n.createLease(lease)
	} else {
		err = n.updateLease(lease)
	}

	if err != nil {
		return err
	}

	if t != nil {
		*t = lease
	}

	log.Printf("new: topic=%s key=%s lessee=%s ttl=%s\n", lease.Topic, lease.Key, lease.Lessee, lease.TTL.Duration())

	return nil
}

func (n *Node) RenewLock(req LockRequest, t *Lease) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	_, ok := n.frozen[req.Topic]
	if ok {
		return ErrFrozenTopic
	}

	lease, err := n.getLease(req.Topic, req.Key)

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
	lease.TTL = sqle.Duration(req.TTL)

	err = n.updateLease(lease)
	if err != nil {
		return err
	}
	if t != nil {
		*t = lease
	}

	return nil
}

func (n *Node) ReleaseLock(req LockRequest, ok *bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	lease, err := n.getLease(req.Topic, req.Key)
	if err != nil {
		if errors.Is(err, ErrNoLease) {
			return nil
		}
		return err
	}

	if lease.Lessee != req.ID {
		return ErrNotYourLease
	}

	err = n.deleteLease(req.Topic, req.Key)

	if err != nil {
		return err
	}

	*ok = true

	log.Printf("release: topic=%s key=%s lessee=%s ttl=%s\n", lease.Topic, lease.Key, lease.Lessee, lease.TTL.Duration())

	return nil
}

func (n *Node) Freeze(topic string, ok *bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.frozen[topic] = struct{}{}
	*ok = true

	log.Printf("freeze: topic=%s", topic)

	return nil
}

func (n *Node) Reset(topic string, ok *bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.frozen, topic)

	*ok = true

	log.Printf("reset: topic=%s", topic)
	return nil
}
