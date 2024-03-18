package dlm

import (
	"context"
	"log/slog"
	"net/rpc"
	"slices"
	"time"

	"github.com/hashicorp/raft"
	"github.com/yaitoo/async"
)

type Cluster struct {
	terms map[string]time.Duration
	peers []Peer
}

func (c *Cluster) Start(n *Node, options ...ClusterOption) error {

	for _, o := range options {
		o(c)
	}

	if len(n.raft.GetConfiguration().Configuration().Servers) == 0 {

		configuration := raft.Configuration{}

		configuration.Servers = []raft.Server{
			{
				ID:      raft.ServerID(n.raftID),
				Address: raft.ServerAddress(n.raftAddr),
			},
		}

		f := n.raft.BootstrapCluster(configuration)

		err := f.Error()
		if err != nil {
			return err
		}
	}

	if !slices.ContainsFunc(c.peers, func(p Peer) bool {
		return p.RaftID == n.raftID
	}) {
		c.peers = append(c.peers, Peer{RaftID: n.raftID, RaftAddr: n.raftAddr, Addr: n.addr})
	}

	ok := <-n.raft.LeaderCh()
	if ok {
		var err error
		for k, v := range c.terms {
			req := TermsRequest{Topic: k, TTL: v}
			err = n.SetTerms(req, &ok)
			if err != nil {
				return err
			}
		}

		return nil
	}

	return ErrClusterNotStarted
}

func (c *Cluster) Join(ctx context.Context, p Peer) error {
	a := async.NewA()

	var exists bool
	for _, peer := range c.peers {

		if peer.RaftID == p.RaftID {
			exists = true
		}

		c, err := connect(ctx, peer.Addr, DefaultRaftTimeout)
		if err != nil {
			Logger.Warn("dlm: peers are unreachable", slog.String("peer", peer.Addr))
			continue
		}

		a.Add(func(c *rpc.Client) func(ctx context.Context) error {
			return func(ctx context.Context) error {
				var s bool
				err := c.Call("dlm.Join", p, &s)
				return err
			}
		}(c))
	}

	_, err := a.WaitAny(ctx)

	if err != nil && !exists {
		c.peers = append(c.peers, p)
	}

	return err

}
