package dlm

import (
	"time"
)

type MutexOption func(m *Mutex)

func WithPeers(peers ...string) MutexOption {
	return func(m *Mutex) {
		m.peers = peers
	}
}

func WithTTL(d time.Duration) MutexOption {
	return func(m *Mutex) {
		m.ttl = d
	}
}

func WithTimeout(d time.Duration) MutexOption {
	return func(m *Mutex) {
		m.timeout = d
	}
}
