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

func (l *Lease) IsLive() bool {
	return time.Now().Before(time.Unix(l.Since, 0).Add(l.TTL.Duration()))
}
