package types

const (
	BackfillTransportWebSocket = "websocket"
	BackfillTransportHTTP      = "http"
)

type BackfillCmd struct {
	Start   int64
	End     int64
	Options []BackfillOption
}

type BackfillOption struct {
	Transport string
	RPCMethod string
	Params    map[string]any
}
