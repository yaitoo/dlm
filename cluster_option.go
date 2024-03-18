package dlm

import "time"

type ClusterOption func(c *Cluster)

func WithLeaseTerms(terms map[string]time.Duration) ClusterOption {
	return func(c *Cluster) {
		c.terms = terms
	}
}

func WithPeers(peers ...Peer) ClusterOption {
	return func(c *Cluster) {
		c.peers = peers
	}
}
