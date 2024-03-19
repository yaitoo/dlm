package dlm

import "time"

type LockRequest struct {
	ID    string
	Topic string
	Key   string
	TTL   time.Duration
}
