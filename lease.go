package dlm

import (
	"time"

	"github.com/yaitoo/sqle"
)

// Lease lock period on the key
type Lease struct {
	Topic string
	Key   string

	Lessee string
	Since  int64
	TTL    sqle.Duration

	// Only available on mutex
	ExpiresOn time.Time `json:"-"`
}

// IsLive check if lease is live on node side
func (l *Lease) IsLive() bool {
	return time.Now().Before(time.Unix(l.Since, 0).Add(l.TTL.Duration()))
}

// IsExpired check if lease expires on mutex side
func (l *Lease) IsExpired(start time.Time) bool {
	now := time.Now()
	l.ExpiresOn = now.Add(l.TTL.Duration() - time.Until(start))
	return !now.Before(l.ExpiresOn)
}
