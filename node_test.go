package dlm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := NewNode(getFreeAddr(), WithRaft("1", getFreeAddr()))
	err := n.Start(ctx)
	require.NoError(t, err)

	// err = n.Serve(ctx)
	// require.NoError(t, err)

	// n2 := NewNode(getFreeAddr(), WithRaft("2", getFreeAddr()))
	// err = n2.Start(ctx)
	// require.NoError(t, err)

	// err = n2.Serve(ctx)
	// require.NoError(t, err)

	walletTerms := 5 * time.Second
	userTerms := 3 * time.Second

	c := &Cluster{}
	err = c.Start(n, WithLeaseTerms(map[string]time.Duration{
		"wallet": walletTerms,
		"user":   userTerms,
	}))
	require.NoError(t, err)

	// err = c.Join(ctx, Peer{Addr: n2.addr, RaftID: n2.raftID, RaftAddr: n2.raftAddr})
	// require.NoError(t, err)

	tests := []struct {
		name string
		run  func(*require.Assertions)
	}{
		{
			name: "new_lock_should_work",
			run: func(re *require.Assertions) {
				var expected Lease
				err := n.NewLock(LockRequest{
					ID:    "new_lock",
					Topic: "wallet",
					Key:   "new_lock",
				}, &expected)

				re.NoError(err)

				actual, err := n.getLease("wallet:new_lock")
				re.NoError(err)
				re.Equal(expected, actual)
				re.Equal(walletTerms, actual.TTL)
				re.Equal("new_lock", actual.Lessee)
				re.Equal("wallet", actual.Topic)
				re.Equal("new_lock", actual.Key)

				err = n.NewLock(LockRequest{
					ID:    "new_lock_2",
					Topic: "wallet",
					Key:   "new_lock",
				}, &expected)

				re.ErrorIs(err, ErrAlreadyLocked)

				err = n.NewLock(LockRequest{
					ID:    "new_lock_3",
					Topic: "no_exists_topic",
					Key:   "new_lock",
				}, &expected)

				re.NoError(err)

				actual, err = n.getLease("no_exists_topic:new_lock")
				re.NoError(err)

				re.Equal(DefaultLeaseTerm, actual.TTL)
				re.Equal("new_lock_3", actual.Lessee)
				re.Equal("no_exists_topic", actual.Topic)
				re.Equal("new_lock", actual.Key)
			},
		},
		{
			name: "renew_lock_should_work",
			run: func(re *require.Assertions) {
				var expected Lease
				err := n.NewLock(LockRequest{
					ID:    "renew_lock",
					Topic: "user",
					Key:   "renew_lock",
				}, &expected)

				re.NoError(err)

				actual, err := n.getLease("user:renew_lock")
				re.NoError(err)
				re.Equal(expected, actual)
				re.Equal(userTerms, actual.TTL)
				re.Equal("renew_lock", actual.Lessee)
				re.Equal("user", actual.Topic)
				re.Equal("renew_lock", actual.Key)

				err = n.RenewLock(LockRequest{
					ID:    "renew_lock_3",
					Topic: "user",
					Key:   "renew_lock",
				}, &expected)

				re.ErrorIs(err, ErrNotYourLease)

				err = n.RenewLock(LockRequest{
					ID:    "renew_lock",
					Topic: "user",
					Key:   "renew_lock",
				}, &expected)

				re.NoError(err)

				actual, err = n.getLease("user:renew_lock")
				re.NoError(err)
				re.Equal(expected, actual)
				re.Equal(userTerms, actual.TTL)
				re.Equal("renew_lock", actual.Lessee)
				re.Equal("user", actual.Topic)
				re.Equal("renew_lock", actual.Key)

				err = n.RenewLock(LockRequest{
					ID:    "renew_lock_3",
					Topic: "user",
					Key:   "renew_lock_2",
				}, &expected)

				re.ErrorIs(err, ErrNoLease)

				time.Sleep(userTerms)
				err = n.RenewLock(LockRequest{
					ID:    "renew_lock",
					Topic: "user",
					Key:   "renew_lock",
				}, &expected)

				re.ErrorIs(err, ErrExpiredLease)

			},
		},
		{
			name: "release_lock_should_work",
			run: func(re *require.Assertions) {
				var expected Lease
				err := n.NewLock(LockRequest{
					ID:    "release_lock",
					Topic: "wallet",
					Key:   "release_lock",
				}, &expected)

				re.NoError(err)

				actual, err := n.getLease("wallet:release_lock")
				re.NoError(err)
				re.Equal(expected, actual)
				re.Equal(walletTerms, actual.TTL)
				re.Equal("release_lock", actual.Lessee)
				re.Equal("wallet", actual.Topic)
				re.Equal("release_lock", actual.Key)
				var ok bool
				err = n.ReleaseLock(LockRequest{
					ID:    "release_lock_3",
					Topic: "wallet",
					Key:   "release_lock",
				}, &ok)

				re.ErrorIs(err, ErrNotYourLease)

				err = n.ReleaseLock(LockRequest{
					ID:    "release_lock",
					Topic: "wallet",
					Key:   "release_lock",
				}, &ok)

				re.NoError(err)

				actual, err = n.getLease("wallet:release_lock")
				re.ErrorIs(err, ErrNoLease)

			},
		},
		{
			name: "terms_should_work",
			run: func(re *require.Assertions) {
				orderTerms := 4 * time.Second
				var ok bool

				actual, err := n.getTerms("order")
				re.NoError(err)
				re.Equal(DefaultLeaseTerm, actual)

				err = n.SetTerms(TermsRequest{
					Topic: "order",
					TTL:   orderTerms,
				}, &ok)

				re.NoError(err)

				actual, err = n.getTerms("order")
				re.NoError(err)
				re.Equal(orderTerms, actual)

				err = n.RemoveTerms(TermsRequest{
					Topic: "order",
				}, &ok)

				re.NoError(err)

				actual, err = n.getTerms("order")
				re.NoError(err)
				re.Equal(DefaultLeaseTerm, actual)

			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.run(require.New(t))
		})
	}
}

func TestLeader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n1 := NewNode(getFreeAddr(), WithRaft("1", getFreeAddr()))
	err := n1.Start(ctx)
	require.NoError(t, err)

	err = n1.Serve(ctx)
	require.NoError(t, err)

	n2 := NewNode(getFreeAddr(), WithRaft("2", getFreeAddr()))
	err = n2.Start(ctx)
	require.NoError(t, err)

	err = n2.Serve(ctx)
	require.NoError(t, err)

	c := &Cluster{}
	err = c.Start(n1)
	require.NoError(t, err)

	err = c.Join(ctx, Peer{Addr: n2.addr, RaftID: n2.raftID, RaftAddr: n2.raftAddr})
	require.NoError(t, err)

	nodes := map[string]*Node{
		"1": n1,
		"2": n2,
	}

	tests := []struct {
		name string
		run  func(*require.Assertions)
	}{
		{
			name: "leader_should_work",
			run: func(re *require.Assertions) {
				var expected Lease

				_, id := n1.raft.LeaderWithID()
				n := nodes[string(id)]

				err := n.NewLock(LockRequest{
					ID:    "leader",
					Topic: "wallet",
					Key:   "leader",
				}, &expected)

				re.NoError(err)

				err = n.RenewLock(LockRequest{
					ID:    "leader",
					Topic: "wallet",
					Key:   "leader",
				}, &expected)
				re.NoError(err)
				var ok bool
				err = n.ReleaseLock(LockRequest{
					ID:    "leader",
					Topic: "wallet",
					Key:   "leader",
				}, &ok)
				re.NoError(err)

				err = n.SetTerms(TermsRequest{
					Topic: "leader",
					TTL:   DefaultLeaseTerm,
				}, &ok)

				re.NoError(err)

				err = n.RemoveTerms(TermsRequest{
					Topic: "leader",
				}, &ok)

				re.NoError(err)
			},
		},
		{
			name: "not_leader_should_not_work",
			run: func(re *require.Assertions) {
				var expected Lease

				_, id := n1.raft.LeaderWithID()
				var n *Node
				if id == "1" {
					n = n2
				} else {
					n = n1
				}

				err := n.NewLock(LockRequest{
					ID:    "follower",
					Topic: "wallet",
					Key:   "follower",
				}, &expected)

				re.ErrorIs(err, ErrNotRaftLeader)

				err = n.RenewLock(LockRequest{
					ID:    "follower",
					Topic: "wallet",
					Key:   "follower",
				}, &expected)
				re.ErrorIs(err, ErrNotRaftLeader)

				var ok bool
				err = n.ReleaseLock(LockRequest{
					ID:    "follower",
					Topic: "wallet",
					Key:   "follower",
				}, &ok)
				re.ErrorIs(err, ErrNotRaftLeader)

				err = n.SetTerms(TermsRequest{
					Topic: "follower",
					TTL:   DefaultLeaseTerm,
				}, &ok)
				re.ErrorIs(err, ErrNotRaftLeader)

				err = n.RemoveTerms(TermsRequest{
					Topic: "follower",
				}, &ok)
				re.ErrorIs(err, ErrNotRaftLeader)

			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.run(require.New(t))
		})
	}
}
