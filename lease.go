package dlm

import "time"

// Lease lock period on the key
type Lease struct {
	Topic string
	Key   string

	Lessee string
	Since  int64
	TTL    time.Duration
	Nonce  uint64

	// Only available on mutex
	ExpiresOn time.Time `json:"-"`
}

func (l *Lease) IsLive() bool {
	return time.Now().Before(time.Unix(l.Since, 0).Add(l.TTL))
}
