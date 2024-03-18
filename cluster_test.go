package dlm

import (
	"context"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func getFreeAddr() string {
	l, err := net.Listen("tcp", ":0")

	if err != nil {
		return ""
	}

	defer l.Close()
	return "127.0.0.1:" + strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}

func TestCluster(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(ctx context.Context) ([]*Node, error)
		assert func(re *require.Assertions, nodes []*Node)
	}{
		{
			name: "single_node_should_work",
			setup: func(ctx context.Context) ([]*Node, error) {
				n := NewNode(getFreeAddr(), WithRaft("1", getFreeAddr()), WithDataDir("./.db"), WithLogger(os.Stderr))
				err := n.Start(ctx)
				if err != nil {
					return nil, err
				}

				c := &Cluster{}
				err = c.Start(n)
				if err != nil {
					return nil, err
				}

				return []*Node{n}, nil
			},
			assert: func(re *require.Assertions, nodes []*Node) {
				re.True(nodes[0].raft.State() == raft.Leader)
			},
		},
		{
			name: "multi_nodes_should_work",
			setup: func(ctx context.Context) ([]*Node, error) {

				var peers []Peer
				for i := 0; i < 9; i++ {
					peers = append(peers, Peer{
						RaftID:   "n" + strconv.Itoa(i),
						RaftAddr: getFreeAddr(),
						Addr:     getFreeAddr(),
					})
				}

				var err error

				var nodes []*Node
				for i := 0; i < 9; i++ {
					n := NewNode(peers[i].Addr, WithRaft(peers[i].RaftID, peers[i].RaftAddr), WithLogger(os.Stderr))
					err = n.Start(ctx)

					if err != nil {
						return nil, err
					}

					err = n.Serve(ctx)
					if err != nil {
						return nil, err
					}

					nodes = append(nodes, n)
				}

				c := &Cluster{}
				err = c.Start(nodes[0], WithPeers(peers[0:3]...))
				if err != nil {
					return nil, err
				}

				for i := 1; i < 9; i++ {
					err = c.Join(ctx, peers[i])
					if err != nil {
						return nil, err
					}

				}

				return nodes, nil
			},
			assert: func(re *require.Assertions, nodes []*Node) {

				var id string

				for _, n := range nodes {
					if n.raft.State() == raft.Leader {

						if id == "" {
							id = n.raftID
						} else {
							re.Fail("leader is not unique")
						}
					}

				}

				re.NotEmpty(id, "leader is empty")
			},
		},
		{
			name: "with_terms_should_work",
			setup: func(ctx context.Context) ([]*Node, error) {
				n := NewNode(getFreeAddr(), WithRaft("1", getFreeAddr()), WithLogger(os.Stderr))
				err := n.Start(ctx)
				if err != nil {
					return nil, err
				}

				c := &Cluster{}
				err = c.Start(n, WithLeaseTerms(map[string]time.Duration{
					"wallet": 5 * time.Second,
					"user":   3 * time.Second,
				}))
				if err != nil {
					return nil, err
				}

				return []*Node{n}, nil

			},
			assert: func(re *require.Assertions, nodes []*Node) {

				leader := nodes[0]

				re.True(leader.raft.State() == raft.Leader)
				ttl, err := leader.getTerms("wallet")
				re.NoError(err)
				re.Equal(5*time.Second, ttl)

				ttl, err = leader.getTerms("user")
				re.NoError(err)
				re.Equal(3*time.Second, ttl)

			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			re := require.New(t)
			nodes, err := test.setup(ctx)
			re.NoError(err)

			test.assert(re, nodes)

			cancel()
			os.RemoveAll("./.db")

		})
	}

}
