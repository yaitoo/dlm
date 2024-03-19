package dlm

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"

	"github.com/yaitoo/sqle"
)

func (n *Node) createLease(l Lease) error {
	cmd := sqle.New().
		Insert("dlm_lease").
		Set("topic", l.Topic).
		Set("key", l.Key).
		Set("lessee", l.Lessee).
		Set("since", l.Since).
		Set("ttl", l.TTL).End()

	_, err := n.db.ExecBuilder(context.Background(), cmd)
	if err != nil {
		n.logger.Warn("dlm: create lease", slog.String("err", err.Error()))
		return ErrBadDatabase
	}

	return nil
}

func (n *Node) updateLease(l Lease) error {
	cmd := sqle.New().
		Update("dlm_lease").
		Set("topic", l.Topic).
		Set("key", l.Key).
		Set("lessee", l.Lessee).
		Set("since", l.Since).
		Set("ttl", l.TTL).
		Where("topic = {topic} and key ={key}").End()

	_, err := n.db.ExecBuilder(context.Background(), cmd)
	if err != nil {
		n.logger.Warn("dlm: update lease", slog.String("err", err.Error()))
		return ErrBadDatabase
	}

	return nil
}

func (n *Node) getLease(topic, key string) (Lease, error) {
	var l Lease

	cmd := sqle.New().
		Select("dlm_lease").
		Where("topic = {topic} AND key = {key}").
		Param("topic", topic).
		Param("key", key)

	err := n.db.QueryRowBuilder(context.Background(), cmd).Bind(&l)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return l, ErrNoLease
		}

		n.logger.Warn("dlm: get lease", slog.String("err", err.Error()))
		return l, ErrBadDatabase
	}

	return l, nil
}

func (n *Node) deleteLease(topic, key string) error {
	_, err := n.db.ExecBuilder(context.Background(), sqle.New().
		Delete("dlm_lease").
		Where("topic = {topic} AND key = {key}").
		Param("topic", topic).
		Param("key", key))
	if err != nil {
		n.logger.Warn("dlm: get lease", slog.String("err", err.Error()))
		return ErrBadDatabase
	}

	return nil
}
