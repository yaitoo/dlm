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
				err := m.Lock(context.TODO())
				r.NoError(err)
				r.Equal(10*time.Second, m.lease.TTL.Duration())
				r.Equal("wallet", m.lease.Topic)
				r.Equal("lock_should_work", m.lease.Key)

			},
		},
		{
			name: "lock_should_not_work_if_lease_exists",
			run: func(r *require.Assertions) {
				m := New("lock", "wallet", "lock_exists", WithPeers(peers...), WithTTL(10*time.Second))
				err := m.Lock(context.TODO())
				r.NoError(err)
				r.Equal(10*time.Second, m.lease.TTL.Duration())
				r.Equal("wallet", m.lease.Topic)
				r.Equal("lock_exists", m.lease.Key)

				m2 := New("lock_2", "wallet", "lock_exists", WithPeers(peers...))
				err = m2.Lock(context.TODO())

				r.Error(err, ErrLeaseExists)
			},
		},
		{
			name: "expires_should_work",
			run: func(r *require.Assertions) {
				ttl := 3 * time.Second
				m := New("lock_should_work", "wallet", "expires_should_work", WithPeers(peers...), WithTTL(ttl))
				err := m.Lock(context.TODO())
				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("wallet", m.lease.Topic)
				r.Equal("expires_should_work", m.lease.Key)

				time.Sleep(ttl)

				<-m.Done()
				r.ErrorIs(context.Cause(m), ErrExpiredLease)

			},
		},
		{
			name: "lock_should_work_when_old_lease_expires",
			run: func(r *require.Assertions) {
				ttl := 3 * time.Second
				m := New("lock", "wallet", "lock_exists", WithPeers(peers...), WithTTL(ttl))
				err := m.Lock(context.TODO())
				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("lock", m.lease.Lessee)
				r.Equal("wallet", m.lease.Topic)
				r.Equal("lock_exists", m.lease.Key)

				ttl2 := 5 * time.Second
				m2 := New("lock_2", "wallet", "lock_exists", WithPeers(peers...), WithTTL(ttl2))
				err = m2.Lock(context.TODO())

				r.Error(err, ErrLeaseExists)

				time.Sleep(ttl) // wait for 1st lease expires

				err = m2.Lock(context.TODO())
				r.NoError(err)
				r.Equal(ttl2, m2.lease.TTL.Duration())
				r.Equal("lock_2", m2.lease.Lessee)
				r.Equal("wallet", m2.lease.Topic)
				r.Equal("lock_exists", m2.lease.Key)
			},
		},
		{
			name: "lock_should_work_when_minority_nodes_are_down",
			run: func(r *require.Assertions) {
				m := New("lock_should_work", "wallet", "minority_nodes_are_down", WithPeers(peers...), WithTTL(10*time.Second))

				nodes[0].Stop()
				nodes[1].Stop()

				err := m.Lock(context.TODO())

				r.NoError(err)
				r.Equal(10*time.Second, m.lease.TTL.Duration())
				r.Equal("wallet", m.lease.Topic)
				r.Equal("minority_nodes_are_down", m.lease.Key)

				m2 := New("lock_should_work_2", "wallet", "minority_nodes_are_down", WithPeers(peers...))
				err = m2.Lock(context.TODO())

				r.Error(err, async.ErrTooLessDone)
			},
		},
		{
			name: "lock_should_not_work_when_majority_nodes_are_down",
			run: func(r *require.Assertions) {
				m := New("lock_should_work", "wallet", "majority_nodes_are_down", WithPeers(peers...), WithTTL(10*time.Second))

				nodes[0].Stop()
				nodes[1].Stop()
				nodes[2].Stop()

				err = m.Lock(context.TODO())
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
				err := m.Lock(context.TODO())

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
				m := New("renew", "wallet", "renew_expires", WithPeers(peers...), WithTTL(ttl))
				err := m.Lock(context.TODO())

				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("wallet", m.lease.Topic)
				r.Equal("renew_expires", m.lease.Key)

				time.Sleep(ttl)

				err = m.Renew(context.TODO())
				r.ErrorIs(err, ErrExpiredLease)

			},
		},
		{
			name: "keepalive_should_work",
			run: func(r *require.Assertions) {
				ttl := 2 * time.Second
				m := New("renew", "wallet", "renew_keepalive", WithPeers(peers...), WithTTL(ttl))
				err := m.Lock(context.TODO())

				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("renew", m.lease.Lessee)
				r.Equal("wallet", m.lease.Topic)
				r.Equal("renew_keepalive", m.lease.Key)

				go m.Keepalive()

				time.Sleep(ttl)

				err = m.Renew(context.TODO())
				r.NoError(err)

				time.Sleep(1 * time.Second)
				err = m.Renew(context.TODO())
				r.NoError(err)

			},
		},
		{
			name: "renew_should_not_work_when_the_lease_does_not_exists",
			run: func(r *require.Assertions) {
				ttl := 2 * time.Second
				m := New("renew", "wallet", "renew_does_not_exists", WithPeers(peers...), WithTTL(ttl))

				err = m.Renew(context.TODO())
				r.Error(err, ErrNoLease)
			},
		},
		{
			name: "renew_should_not_work_when_lease_is_not_yours",
			run: func(r *require.Assertions) {
				ttl := 5 * time.Second
				m := New("renew", "wallet", "renew_not_yours", WithPeers(peers...), WithTTL(ttl))

				err := m.Lock(context.Background())
				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("renew", m.lease.Lessee)
				r.Equal("wallet", m.lease.Topic)
				r.Equal("renew_not_yours", m.lease.Key)

				m2 := New("renew_2", "wallet", "renew_not_yours", WithPeers(peers...), WithTTL(ttl))
				err = m2.Renew(context.Background())
				r.ErrorIs(err, ErrNotYourLease)

			},
		},
		{
			name: "renew_should_work_when_minority_nodes_are_down",
			run: func(r *require.Assertions) {
				m := New("renew", "wallet", "renew_minority", WithPeers(peers...), WithTTL(10*time.Second))

				err := m.Lock(context.TODO())

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

				err := m.Lock(context.TODO())

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

func TestUnlock(t *testing.T) {
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
			name: "unlock_should_work",
			run: func(r *require.Assertions) {
				ttl := 10 * time.Second
				m := New("unlock", "wallet", "unlock", WithPeers(peers...), WithTTL(ttl))
				err := m.Lock(context.TODO())

				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("wallet", m.lease.Topic)
				r.Equal("unlock", m.lease.Key)

				err = m.Unlock(context.TODO())
				r.NoError(err)

				err = m.Renew(context.TODO())
				r.ErrorIs(err, ErrNoLease)
			},
		},
		{
			name: "unlock_should_not_work_when_lease_is_not_yours",
			run: func(r *require.Assertions) {
				ttl := 10 * time.Second
				m := New("unlock", "wallet", "unlock_not_yours", WithPeers(peers...), WithTTL(ttl))
				err := m.Lock(context.TODO())

				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("unlock", m.lease.Lessee)
				r.Equal("wallet", m.lease.Topic)
				r.Equal("unlock_not_yours", m.lease.Key)

				m2 := New("unlock_2", "wallet", "unlock_not_yours", WithPeers(peers...), WithTTL(ttl))
				err = m2.Unlock(context.TODO())

				r.ErrorIs(err, ErrNotYourLease)
			},
		},

		{
			name: "unlock_should_work_when_minority_nodes_are_down",
			run: func(r *require.Assertions) {
				ttl := 10 * time.Second
				m := New("unlock", "wallet", "unlock_minority", WithPeers(peers...), WithTTL(ttl))

				err := m.Lock(context.TODO())

				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("unlock", m.lease.Lessee)
				r.Equal("wallet", m.lease.Topic)
				r.Equal("unlock_minority", m.lease.Key)

				nodes[0].Stop()
				nodes[1].Stop()
				err = m.Unlock(context.TODO())
				r.NoError(err)

			},
		},
		{
			name: "renew_should_not_work_when_majority_nodes_are_down",
			run: func(r *require.Assertions) {
				ttl := 10 * time.Second
				m := New("unlock", "wallet", "unlock_majority", WithPeers(peers...), WithTTL(ttl))

				err := m.Lock(context.TODO())

				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("unlock", m.lease.Lessee)
				r.Equal("wallet", m.lease.Topic)
				r.Equal("unlock_majority", m.lease.Key)

				nodes[0].Stop()
				nodes[1].Stop()
				nodes[2].Stop()

				err = m.Unlock(context.TODO())
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
