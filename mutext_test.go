package dlm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yaitoo/async"
	"github.com/yaitoo/sqle"
)

func createCluster(num int) ([]string, []*Node, func(), error) {
	var peers []string
	var nodes []*Node
	var clean []func()

	release := func() {
		for _, c := range clean {
			c()
		}
	}

	for i := 0; i < num; i++ {
		db, fn, err := createSqlite3()
		if err != nil {
			return nil, nil, release, err
		}
		clean = append(clean, fn)
		n := NewNode(getFreeAddr(), sqle.Open(db))
		err = n.Start()
		if err != nil {
			return nil, nil, release, err
		}
		peers = append(peers, n.addr)
		nodes = append(nodes, n)

		clean = append(clean, n.Stop)
	}

	return peers, nodes, release, nil
}

func TestLock(t *testing.T) {
	peers, nodes, clean, err := createCluster(5)
	require.NoError(t, err)
	defer clean()

	tests := []struct {
		name string
		run  func(*require.Assertions)
	}{
		{
			name: "lock_should_work",
			run: func(r *require.Assertions) {
				m := New("lock_should_work", "wallet", "lock_should_work", WithPeers(peers...), WithTTL(10*time.Second), WithTimeout(5*time.Second))
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
				defer nodes[0].Start() // nolint: errcheck
				nodes[1].Stop()
				defer nodes[1].Start() // nolint: errcheck

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
				defer nodes[0].Start() // nolint: errcheck
				nodes[1].Stop()
				defer nodes[1].Start() // nolint: errcheck
				nodes[2].Stop()
				defer nodes[2].Start() // nolint: errcheck

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
	peers, nodes, clean, err := createCluster(5)
	require.NoError(t, err)
	defer clean()

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
				defer nodes[0].Start() // nolint: errcheck
				nodes[1].Stop()
				defer nodes[1].Start() // nolint: errcheck
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
				defer nodes[0].Start() // nolint: errcheck
				nodes[1].Stop()
				defer nodes[1].Start() // nolint: errcheck
				nodes[2].Stop()
				defer nodes[2].Start() // nolint: errcheck

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
	peers, nodes, clean, err := createCluster(5)
	require.NoError(t, err)
	defer clean()

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
			name: "unlock_should_work_even_lease_does_not_exists",
			run: func(r *require.Assertions) {
				ttl := 10 * time.Second
				m := New("unlock", "wallet", "unlock", WithPeers(peers...), WithTTL(ttl))

				err = m.Unlock(context.TODO())
				r.NoError(err)

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
				defer nodes[0].Start() // nolint: errcheck

				nodes[1].Stop()
				defer nodes[1].Start() // nolint: errcheck

				err = m.Unlock(context.TODO())
				r.NoError(err)

			},
		},
		{
			name: "unlock_should_not_work_when_majority_nodes_are_down",
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
				defer nodes[0].Start() // nolint: errcheck
				nodes[1].Stop()
				defer nodes[1].Start() // nolint: errcheck
				nodes[2].Stop()
				defer nodes[2].Start() // nolint: errcheck

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

func TestTopic(t *testing.T) {
	peers, nodes, clean, err := createCluster(5)
	require.NoError(t, err)
	defer clean()

	tests := []struct {
		name string
		run  func(*require.Assertions)
	}{
		{
			name: "freeze_should_work",
			run: func(r *require.Assertions) {
				ttl := 10 * time.Second
				m := New("freeze", "freeze", "freeze", WithPeers(peers...), WithTTL(ttl))
				err := m.Lock(context.TODO())

				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("freeze", m.lease.Lessee)
				r.Equal("freeze", m.lease.Topic)
				r.Equal("freeze", m.lease.Key)

				err = m.Freeze(context.Background(), "freeze")
				r.NoError(err)

				err = m.Renew(context.TODO())
				r.ErrorIs(err, ErrFrozenTopic)

				m2 := New("freeze_2", "freeze", "freeze_2", WithPeers(peers...), WithTTL(ttl))
				err = m2.Lock(context.TODO())
				r.ErrorIs(err, ErrFrozenTopic)

				err = m.Reset(context.Background(), "freeze")
				r.NoError(err)

				err = m.Renew(context.Background())
				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("freeze", m.lease.Topic)
				r.Equal("freeze", m.lease.Key)

				err = m2.Lock(context.TODO())
				r.NoError(err)
				r.Equal(ttl, m2.lease.TTL.Duration())
				r.Equal("freeze_2", m2.lease.Lessee)
				r.Equal("freeze", m2.lease.Topic)
				r.Equal("freeze_2", m2.lease.Key)
			},
		},
		{
			name: "freeze_should_work_when_minority_nodes_are_down",
			run: func(r *require.Assertions) {
				ttl := 10 * time.Second
				m := New("freeze", "freeze", "freeze", WithPeers(peers...), WithTTL(ttl))
				err := m.Lock(context.TODO())

				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("freeze", m.lease.Lessee)
				r.Equal("freeze", m.lease.Topic)
				r.Equal("freeze", m.lease.Key)

				nodes[0].Stop()
				defer nodes[0].Start() // nolint: errcheck
				nodes[1].Stop()
				defer nodes[1].Start() // nolint: errcheck

				err = m.Freeze(context.Background(), "freeze")
				r.NoError(err)

				err = m.Renew(context.TODO())
				r.ErrorIs(err, ErrFrozenTopic)

				m2 := New("freeze_2", "freeze", "freeze_2", WithPeers(peers...), WithTTL(ttl))
				err = m2.Lock(context.TODO())
				r.ErrorIs(err, ErrFrozenTopic)

				err = m.Reset(context.Background(), "freeze")
				r.NoError(err)

				err = m.Renew(context.Background())
				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("freeze", m.lease.Topic)
				r.Equal("freeze", m.lease.Key)

				err = m2.Lock(context.TODO())
				r.NoError(err)
				r.Equal(ttl, m2.lease.TTL.Duration())
				r.Equal("freeze_2", m2.lease.Lessee)
				r.Equal("freeze", m2.lease.Topic)
				r.Equal("freeze_2", m2.lease.Key)
			},
		},

		{
			name: "freeze_should_not_work_when_majority_nodes_are_down",
			run: func(r *require.Assertions) {
				ttl := 10 * time.Second
				m := New("freeze", "freeze", "freeze", WithPeers(peers...), WithTTL(ttl))
				err := m.Lock(context.TODO())

				r.NoError(err)
				r.Equal(ttl, m.lease.TTL.Duration())
				r.Equal("freeze", m.lease.Lessee)
				r.Equal("freeze", m.lease.Topic)
				r.Equal("freeze", m.lease.Key)

				nodes[0].Stop()
				defer nodes[0].Start() // nolint: errcheck
				nodes[1].Stop()
				defer nodes[1].Start() // nolint: errcheck
				nodes[2].Stop()
				defer nodes[2].Start() // nolint: errcheck

				err = m.Freeze(context.Background(), "freeze")
				r.ErrorIs(err, async.ErrTooLessDone)

				err = m.Reset(context.Background(), "freeze")
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
