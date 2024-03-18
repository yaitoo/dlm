package dlm

import "time"

type MutexOption func(m *Mutex)

func WithDispatcher(dispatchers ...string) MutexOption {
	return func(m *Mutex) {
		m.dispatchers = dispatchers
	}
}

func WithTimeout(d time.Duration) MutexOption {
	return func(m *Mutex) {
		m.timeout = d
	}
}
