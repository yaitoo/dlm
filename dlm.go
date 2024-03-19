package dlm

import (
	"errors"
	"log/slog"
	"time"
)

var (
	ErrExpiredLease = errors.New("dlm: lease expires")
	ErrNoLease      = errors.New("dlm: no lease")
	ErrNotYourLease = errors.New("dlm: not your lease")
	ErrLeaseExists  = errors.New("dlm: lease exists")

	ErrFrozenTopic = errors.New("dlm: topic is frozen")

	ErrBadDatabase = errors.New("dlm: bad database operation")
)

var (
	DefaultTimeout   = 3 * time.Second
	DefaultLeaseTerm = 5 * time.Second

	Logger = slog.Default()
)

const (
	CreateTableLease = "CREATE TABLE IF NOT EXISTS dlm_lease(" +
		"`topic` varchar(20) NOT NULL," +
		"`key` varchar(50) NOT NULL," +
		"`lessee` varchar(36) NOT NULL," +
		"`since` int NOT NULL DEFAULT '0'," +
		"`ttl` int NOT NULL DEFAULT '0'," +
		"PRIMARY KEY (topic, key));"

	CreateTableTopic = "CREATE TABLE IF NOT EXISTS dlm_topic(" +
		"`topic` varchar(20) NOT NULL," +
		"`ttl` int NOT NULL DEFAULT '0'," +
		"PRIMARY KEY (topic));"
)
