package dlm

import "time"

type LockRequest struct {
	ID    string
	Topic string
	Key   string
}

type TermsRequest struct {
	Topic string
	TTL   time.Duration
}
