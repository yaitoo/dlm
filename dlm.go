package dlm

import (
	"errors"
	"log/slog"
	"time"
)

var (
	ErrExpiredLease      = errors.New("dlm: lease expires")
	ErrNoLease           = errors.New("dlm: no lease")
	ErrNotYourLease      = errors.New("dlm: not your lease")
	ErrStaleNonce        = errors.New("dlm: stale nonce")
	ErrInvalidTopic      = errors.New("dlm: topic can't starts with @")
	ErrNotRaftLeader     = errors.New("dlm: not raft leader")
	ErrAlreadyLocked     = errors.New("dlm: already locked by others")
	ErrNoConsensus       = errors.New("dlm: no consensus")
	ErrClusterNotStarted = errors.New("dlm: cluster is not started")
	ErrFrozenTopic       = errors.New("dlm: topic is frozen")
)

var (
	DefaultRaftTimeout         = 3 * time.Second
	DefaultTimeout             = 3 * time.Second
	DefaultLeaseTerm           = 5 * time.Second
	DefaultRetainSnapshotCount = 2
	Logger                     = slog.Default()
)

const (
	TopicTerms = "@terms:"
)
