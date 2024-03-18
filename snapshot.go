package dlm

import (
	"encoding/json"
	"log/slog"

	"github.com/hashicorp/raft"
)

type snapshot struct {
	store map[string][]byte
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(s.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		if e := sink.Cancel(); e != nil {
			Logger.Warn("cancel snapshot sink", slog.String("err", e.Error()))
		}
	}

	return err
}

func (_ *snapshot) Release() {}
