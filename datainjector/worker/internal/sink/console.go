package sink

import (
	"encoding/json"
	"log"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

type Sink interface {
	Write(msg *types.Message) error
}

// BatchSink 可选接口，支持批量写入以提升吞吐。
type BatchSink interface {
	WriteBatch(msgs []*types.Message) error
}

type Console struct{}

func init() {
	Register("console", func(cfg map[string]any) (Sink, error) {
		return &Console{}, nil
	})
}

func (c *Console) Write(msg *types.Message) error {
	var pretty map[string]any
	if err := json.Unmarshal(msg.Payload, &pretty); err == nil {
		b, _ := json.Marshal(pretty)
		log.Printf("sink.console: %s", string(b))
	} else {
		log.Printf("sink.console: %q", string(msg.Payload))
	}
	return nil
}
