package dlm

import (
	"database/sql"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/yaitoo/sqle"
)

func getFreeAddr() string {
	l, err := net.Listen("tcp", ":0")

	if err != nil {
		return ""
	}

	defer l.Close()
	return "127.0.0.1:" + strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}

func createSqlite3() (*sql.DB, func(), error) {
	f, err := os.CreateTemp(".", "*.db")
	f.Close()

	clean := func() {
		os.Remove(f.Name()) //nolint
	}

	if err != nil {
		return nil, clean, err
	}

	db, err := sql.Open("sqlite3", f.Name())

	if err != nil {
		return nil, clean, err
	}

	return db, clean, nil

}

func TestLease(t *testing.T) {
	db, clean, err := createSqlite3()
	require.NoError(t, err)
	defer clean()
	n := NewNode(getFreeAddr(), sqle.Open(db))
	err = n.Start()
	require.NoError(t, err)
	defer n.Stop()

	walletTerms := sqle.Duration(5 * time.Second)
	userTerms := sqle.Duration(3 * time.Second)

	tests := []struct {
		name string
		run  func(*require.Assertions)
	}{
		{
			name: "new_lock_should_work",
			run: func(re *require.Assertions) {
				var expected Lease
				req := LockRequest{
					ID:    "new_lock",
					Topic: "wallet",
					Key:   "new_lock",
					TTL:   walletTerms.Duration(),
				}

				err := n.NewLock(req, &expected)

				re.NoError(err)

				actual, err := n.getLease("wallet", "new_lock")
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

				re.ErrorIs(err, ErrLeaseExists)

				err = n.NewLock(LockRequest{
					ID:    "new_lock_3",
					Topic: "no_exists_topic",
					Key:   "new_lock",
					TTL:   DefaultLeaseTerm,
				}, &expected)

				re.NoError(err)

				actual, err = n.getLease("no_exists_topic", "new_lock")
				re.NoError(err)

				re.Equal(DefaultLeaseTerm, actual.TTL.Duration())
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
					TTL:   userTerms.Duration(),
				}, &expected)

				re.NoError(err)

				actual, err := n.getLease("user", "renew_lock")
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
					TTL:   userTerms.Duration(),
				}, &expected)

				re.ErrorIs(err, ErrNotYourLease)

				err = n.RenewLock(LockRequest{
					ID:    "renew_lock",
					Topic: "user",
					Key:   "renew_lock",
					TTL:   userTerms.Duration(),
				}, &expected)

				re.NoError(err)

				actual, err = n.getLease("user", "renew_lock")
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

				time.Sleep(userTerms.Duration())
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
					TTL:   walletTerms.Duration(),
				}, &expected)

				re.NoError(err)

				actual, err := n.getLease("wallet", "release_lock")
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

				actual, err = n.getLease("wallet", "release_lock")
				re.ErrorIs(err, ErrNoLease)

			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.run(require.New(t))
		})
	}
}

func TestNodeTopic(t *testing.T) {
	db, clean, err := createSqlite3()
	require.NoError(t, err)
	defer clean()

	n := NewNode(getFreeAddr(), sqle.Open(db))
	err = n.Start()
	require.NoError(t, err)
	defer n.Stop()

	terms := sqle.Duration(5 * time.Second)

	tests := []struct {
		name string
		run  func(*require.Assertions)
	}{
		{
			name: "freeze_should_work",
			run: func(re *require.Assertions) {
				var expected Lease

				err := n.NewLock(LockRequest{
					ID:    "freeze",
					Topic: "freeze",
					Key:   "freeze",
					TTL:   terms.Duration(),
				}, &expected)

				re.NoError(err)

				actual, err := n.getLease("freeze", "freeze")
				re.NoError(err)
				re.Equal(expected, actual)
				re.Equal(terms, actual.TTL)
				re.Equal("freeze", actual.Lessee)
				re.Equal("freeze", actual.Topic)
				re.Equal("freeze", actual.Key)

				var ok bool
				err = n.Freeze("freeze", &ok)
				re.NoError(err)

				err = n.RenewLock(LockRequest{
					ID:    "freeze",
					Topic: "freeze",
					Key:   "freeze",
				}, &expected)
				re.ErrorIs(err, ErrFrozenTopic)

				err = n.NewLock(LockRequest{
					ID:    "freeze",
					Topic: "freeze",
					Key:   "freeze_2",
				}, &expected)

				re.ErrorIs(err, ErrFrozenTopic)

				err = n.Reset("freeze", &ok)
				re.NoError(err)

				err = n.NewLock(LockRequest{
					ID:    "freeze",
					Topic: "freeze",
					Key:   "freeze_3",
					TTL:   terms.Duration(),
				}, &expected)

				re.NoError(err)

				actual, err = n.getLease("freeze", "freeze_3")
				re.NoError(err)

				re.Equal(terms, actual.TTL)
				re.Equal("freeze", actual.Lessee)
				re.Equal("freeze", actual.Topic)
				re.Equal("freeze_3", actual.Key)
			},
		},
		{
			name: "reset_should_work",
			run: func(re *require.Assertions) {
				var expected Lease

				var ok bool
				err = n.Freeze("reset", &ok)
				re.NoError(err)

				err = n.NewLock(LockRequest{
					ID:    "reset",
					Topic: "reset",
					Key:   "reset",
				}, &expected)

				re.ErrorIs(err, ErrFrozenTopic)

				err = n.RenewLock(LockRequest{
					ID:    "reset",
					Topic: "reset",
					Key:   "reset",
				}, &expected)
				re.ErrorIs(err, ErrFrozenTopic)

				err = n.Reset("reset", &ok)
				re.NoError(err)

				err = n.NewLock(LockRequest{
					ID:    "reset",
					Topic: "reset",
					Key:   "reset",
					TTL:   terms.Duration(),
				}, &expected)

				re.NoError(err)

				actual, err := n.getLease("reset", "reset")
				re.NoError(err)

				re.Equal(terms, actual.TTL)
				re.Equal("reset", actual.Lessee)
				re.Equal("reset", actual.Topic)
				re.Equal("reset", actual.Key)

				err = n.RenewLock(LockRequest{
					ID:    "reset",
					Topic: "reset",
					Key:   "reset",
					TTL:   10 * time.Second,
				}, &expected)

				re.NoError(err)

				actual, err = n.getLease("reset", "reset")
				re.NoError(err)

				re.Equal(10*time.Second, actual.TTL.Duration())
				re.Equal("reset", actual.Lessee)
				re.Equal("reset", actual.Topic)
				re.Equal("reset", actual.Key)

			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.run(require.New(t))
		})
	}
}
