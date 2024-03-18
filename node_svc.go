package dlm

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Start starts the node
func (n *Node) Start(ctx context.Context) error {

	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(n.raftID)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", n.raftAddr)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(n.raftAddr, addr, 3, 10*time.Second, n.logger)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	var snapshots raft.SnapshotStore
	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore
	if n.dir == "" {
		snapshots = raft.NewInmemSnapshotStore()
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
	} else {
		_, err := os.Stat(n.dir)
		if errors.Is(err, os.ErrNotExist) {
			err = os.MkdirAll(n.dir, 0700)
			if err != nil {
				return err
			}
		}

		snapshots, err = raft.NewFileSnapshotStore(n.dir, DefaultRetainSnapshotCount, n.logger)
		if err != nil {
			return err
		}

		path := filepath.Join(n.dir, "raft_"+n.raftID+".db")

		boltDB, err := raftboltdb.New(raftboltdb.Options{
			Path: path,
		})
		if err != nil {
			return err
		}
		logStore = boltDB
		stableStore = boltDB
	}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, n, logStore, stableStore, snapshots, transport)
	if err != nil {
		return err
	}
	n.raft = ra

	go func() {
		<-ctx.Done()

		n.raft.Shutdown()
	}()

	return nil
}

// Serve start the node's RPC service
func (n *Node) Serve(ctx context.Context) error {
	l, err := net.Listen("tcp", n.addr)
	if err != nil {
		return err
	}

	n.server = rpc.NewServer()

	go n.waitClose(ctx, l)
	go n.waitRequest(l)

	return n.server.RegisterName("dlm", n)
}

func (n *Node) waitRequest(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err == nil {
			go n.server.ServeConn(conn)
		} else {
			if errors.Is(err, net.ErrClosed) {
				return
			}

			Logger.Warn("wait request", slog.String("err", err.Error()), slog.String("node_id", n.raftID))
		}

	}
}

func (n *Node) waitClose(ctx context.Context, l net.Listener) {
	<-ctx.Done()
	l.Close()
}
