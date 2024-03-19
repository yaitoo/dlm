package dlm

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/rpc"
	"strings"
	"sync"
	"time"

	"github.com/yaitoo/async"
)

func New(id, topic, key string, options ...MutexOption) *Mutex {

	m := &Mutex{
		id:      id,
		topic:   strings.ToLower(topic),
		key:     strings.ToLower(key),
		done:    make(chan struct{}),
		timeout: DefaultTimeout,
		ttl:     DefaultLeaseTerm,
	}

	for _, o := range options {
		o(m)
	}

	m.consensus = int(math.Floor(float64(len(m.peers))/2)) + 1

	return m
}

type Mutex struct {
	mu sync.RWMutex

	id      string
	topic   string
	key     string
	peers   []string
	timeout time.Duration
	ttl     time.Duration

	consensus int
	done      chan struct{}

	lease Lease
}

func (m *Mutex) connect(ctx context.Context) ([]*rpc.Client, error) {

	a := async.New[*rpc.Client]()
	for _, d := range m.peers {
		a.Add(func(addr string) func(context.Context) (*rpc.Client, error) {
			return func(ctx context.Context) (*rpc.Client, error) {
				return connect(ctx, addr, m.timeout)
			}
		}(d))
	}

	cluster, _, err := a.Wait(ctx)
	if len(cluster) >= m.consensus {
		return cluster, nil
	}

	return nil, err

}

func (m *Mutex) Lock(ctx context.Context) (context.Context, context.CancelFunc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	a := async.New[Lease]()
	req := m.createRequest()

	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	cluster, err := m.connect(ctx)
	if err != nil {
		return nil, nil, err
	}

	for _, c := range cluster {
		a.Add(func(c *rpc.Client) func(ctx context.Context) (Lease, error) {
			return func(ctx context.Context) (Lease, error) {
				var t Lease
				err := c.Call("dlm.NewLock", req, &t)
				return t, err
			}
		}(c))
	}

	start := time.Now()
	result, errs, err := a.WaitN(ctx, m.consensus)

	if err != nil {
		Logger.Warn("dlm: renew lock", slog.Any("err", errs))
		return nil, nil, m.Error(errs, err)
	}

	t := result[0]

	if t.IsExpired(start) {
		return nil, nil, ErrExpiredLease
	}

	m.lease = t

	statusCtx, statusCancel := context.WithCancel(context.Background())

	go m.keepalive(statusCtx, statusCancel)
	go m.waitExpires(statusCtx, statusCancel)

	return statusCtx, statusCancel, nil

}

func (m *Mutex) Renew(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	a := async.New[Lease]()
	req := m.createRequest()
	cluster, err := m.connect(ctx)
	if err != nil {
		return err
	}

	for _, c := range cluster {
		a.Add(func(c *rpc.Client) func(ctx context.Context) (Lease, error) {
			return func(ctx context.Context) (Lease, error) {
				var t Lease
				err := c.Call("dlm.RenewLock", req, &t)
				if err != nil {
					return t, err
				}
				return t, nil
			}
		}(c))
	}

	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	start := time.Now()
	result, errs, err := a.WaitN(ctx, m.consensus)
	if err != nil {
		Logger.Warn("dlm: renew lock", slog.Any("err", errs))
		return m.Error(errs, err)
	}

	t := result[0]
	if t.IsExpired(start) {
		return ErrExpiredLease
	}

	m.lease = t
	return nil
}

func (m *Mutex) Unlock(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	a := async.New[bool]()
	req := m.createRequest()

	cluster, err := m.connect(ctx)
	if err != nil {
		return err
	}

	for _, c := range cluster {
		a.Add(func(c *rpc.Client) func(ctx context.Context) (bool, error) {
			return func(ctx context.Context) (bool, error) {
				var t bool
				err := c.Call("dlm.ReleaseLock", req, &t)
				if err != nil {
					return t, err
				}

				return t, nil
			}
		}(c))
	}
	m.done <- struct{}{}

	_, errs, err := a.WaitN(ctx, m.consensus)
	if err != nil {
		Logger.Warn("dlm: renew lock", slog.Any("err", errs))
		return m.Error(errs, err)
	}

	return nil
}
func (m *Mutex) createRequest() LockRequest {
	return LockRequest{
		ID:    m.id,
		Topic: m.topic,
		Key:   m.key,
		TTL:   m.ttl,
	}
}

func (m *Mutex) waitExpires(ctx context.Context, cancel context.CancelFunc) {
	defer cancel()
	var expiresOn time.Time
	for {
		m.mu.RLock()
		expiresOn = m.lease.ExpiresOn
		m.mu.RUnlock()

		select {
		case <-m.done:
			return
		case <-ctx.Done():
			return
		case <-time.After(time.Until(expiresOn)):
			if !expiresOn.Before(expiresOn) {
				return
			}
		}
	}
}

func (m *Mutex) keepalive(ctx context.Context, cancel context.CancelFunc) {
	defer cancel()

	var err error
	for {

		m.mu.RLock()
		expiresOn := m.lease.ExpiresOn
		m.mu.RUnlock()

		select {
		case <-m.done:
			return
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
			// lease already expires
			if !expiresOn.Before(expiresOn) {
				return
			}

			err = m.Renew(context.Background())
			if errors.Is(err, ErrExpiredLease) {
				return
			}
		}
	}
}

// Error try unwrap consensus known error
func (m *Mutex) Error(errs []error, err error) error {
	consensus := make(map[rpc.ServerError]int)

	for _, err := range errs {
		s, ok := err.(rpc.ServerError)
		if ok {
			c, ok := consensus[s]
			if !ok {
				consensus[s] = 1
			} else {
				consensus[s] = c + 1
			}
		}
	}

	max := 0
	var msg string

	for k, v := range consensus {
		if v > max {
			max = v
			msg = string(k)
		}
	}

	if max < m.consensus {
		return err
	}

	if !strings.HasPrefix(msg, "dlm:") {
		return err
	}

	switch msg {
	case ErrExpiredLease.Error():
		return ErrExpiredLease
	case ErrNoLease.Error():
		return ErrNoLease
	case ErrNotYourLease.Error():
		return ErrNotYourLease
	case ErrLeaseExists.Error():
		return ErrLeaseExists
	case ErrFrozenTopic.Error():
		return ErrFrozenTopic
	case ErrBadDatabase.Error():
		return ErrBadDatabase
	}

	return err
}
