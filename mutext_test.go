package dlm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yaitoo/async"
	"github.com/yaitoo/sqle"
)

func createCluster(ctx context.Context, num int) ([]string, []*Node, []func(), error) {
	var peers []string
	var nodes []*Node
	var clean []func()

	for i := 0; i < num; i++ {
		db, fn, err := createSqlite3()
		if err != nil {
			return nil, nil, clean, err
		}
		clean = append(clean, fn)
		n := NewNode(getFreeAddr(), sqle.Open(db))
		err = n.Start(ctx)
		if err != nil {
			return nil, nil, clean, err
		}
		peers = append(peers, n.addr)
		nodes = append(nodes, n)
	}

	return peers, nodes, clean, nil
}

func TestLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	peers, nodes, clean, err := createCluster(ctx, 5)

	require.NoError(t, err)

	defer func() {
		for _, c := range clean {
			c()
		}
	}()

	tests := []struct {
		name string
		run  func(*require.Assertions)
	}{
		{
			name: "lock_should_work",
			run: func(r *require.Assertions) {
				m := New("lock_should_work", "wallet", "lock_should_work", WithPeers(peers...), WithTTL(10*time.Second))
				_, cancel, err := m.Lock(context.TODO())
				defer cancel()
				r.NoError(err)
				r.Equal(10*time.Second, m.lease.TTL.Duration())
				r.Equal("wallet", m.lease.Topic)
				r.Equal("lock_should_work", m.lease.Key)

				m2 := New("lock_should_work_2", "wallet", "lock_should_work", WithPeers(peers...))
				_, _, err = m2.Lock(context.TODO())

				r.Error(err, async.ErrTooLessDone)
			},
		},
		{
			name: "lock_should_work_when_minority_nodes_are_down",
			run: func(r *require.Assertions) {
				m := New("lock_should_work", "wallet", "minority_nodes_are_down", WithPeers(peers...), WithTTL(10*time.Second))

				nodes[0].Stop()
				nodes[1].Stop()

				_, cancel, err := m.Lock(context.TODO())
				defer cancel()
				r.NoError(err)
				r.Equal(10*time.Second, m.lease.TTL.Duration())
				r.Equal("wallet", m.lease.Topic)
				r.Equal("minority_nodes_are_down", m.lease.Key)

				m2 := New("lock_should_work_2", "wallet", "minority_nodes_are_down", WithPeers(peers...))
				_, _, err = m2.Lock(context.TODO())

				r.Error(err, async.ErrTooLessDone)

			},
		},
		{
			name: "lock_should_not_work_when_majority_nodes_are_down",
			run: func(r *require.Assertions) {
				m := New("lock_should_work", "wallet", "majority_nodes_are_down", WithPeers(peers...), WithTTL(10*time.Second))

				nodes[2].Stop()

				_, cancel, err := m.Lock(context.TODO())
				if cancel != nil {
					defer cancel()
				}

				r.Error(err, async.ErrTooLessDone)

			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.run(require.New(t))
		})
	}
}

func TestRenew(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	peers, nodes, clean, err := createCluster(ctx, 5)

	require.NoError(t, err)

	defer func() {
		for _, c := range clean {
			c()
		}
	}()

	tests := []struct {
		name string
		run  func(*require.Assertions)
	}{
		{
			name: "renew_should_work",
			run: func(r *require.Assertions) {
				ttl := 10 * time.Second
				m := New("renew", "wallet", "renew", WithPeers(peers...), WithTTL(ttl))
				_, cancel, err := m.Lock(context.TODO())
				defer cancel()
				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("wallet", m.lease.Topic)
				r.Equal("renew", m.lease.Key)

				err = m.Renew(context.TODO())
				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("wallet", m.lease.Topic)
				r.Equal("renew", m.lease.Key)
			},
		},
		{
			name: "renew_should_not_work_when_lease_is_expired",
			run: func(r *require.Assertions) {
				ttl := 2 * time.Second
				m := New("renew", "wallet", "renew", WithPeers(peers...), WithTTL(ttl))
				_, cancel, err := m.Lock(context.TODO())
				defer cancel()
				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("wallet", m.lease.Topic)
				r.Equal("renew", m.lease.Key)

				time.Sleep(ttl)

				err = m.Renew(context.TODO())
				r.ErrorIs(err, ErrExpiredLease)

			},
		},
		{
			name: "renew_should_work_when_minority_nodes_are_down",
			run: func(r *require.Assertions) {
				m := New("renew", "wallet", "renew_minority", WithPeers(peers...), WithTTL(10*time.Second))

				_, cancel, err := m.Lock(context.TODO())
				defer cancel()
				r.NoError(err)
				r.Equal(10*time.Second, m.lease.TTL.Duration())
				r.Equal("wallet", m.lease.Topic)
				r.Equal("renew_minority", m.lease.Key)

				nodes[0].Stop()
				nodes[1].Stop()
				err = m.Renew(context.TODO())
				r.NoError(err)

			},
		},
		{
			name: "renew_should_not_work_when_majority_nodes_are_down",
			run: func(r *require.Assertions) {
				m := New("renew", "wallet", "renew_majority", WithPeers(peers...), WithTTL(10*time.Second))

				_, cancel, err := m.Lock(context.TODO())
				defer cancel()
				r.NoError(err)
				r.Equal(10*time.Second, m.lease.TTL.Duration())
				r.Equal("wallet", m.lease.Topic)
				r.Equal("renew_majority", m.lease.Key)

				nodes[0].Stop()
				nodes[1].Stop()
				nodes[2].Stop()
				err = m.Renew(context.TODO())
				r.ErrorIs(err, async.ErrTooLessDone)

			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.run(require.New(t))
		})
	}
}
