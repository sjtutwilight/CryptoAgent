package binance

import "encoding/json"

// DecodeDepthMessage parses either combined-stream or single depth payload.
func DecodeDepthMessage(raw []byte) (*DepthMessage, string, error) {
	var combined CombinedDepthMessage
	if err := json.Unmarshal(raw, &combined); err == nil && combined.Data.Symbol != "" {
		return &combined.Data, combined.Stream, nil
	}

	var msg DepthMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, "", err
	}
	return &msg, "", nil
}

// DecodeTradeMessage parses trade payload.
func DecodeTradeMessage(raw []byte) (*TradeMessage, string, error) {
	var combined CombinedTradeMessage
	if err := json.Unmarshal(raw, &combined); err == nil && combined.Data.Symbol != "" {
		return &combined.Data, combined.Stream, nil
	}

	var msg TradeMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, "", err
	}
	return &msg, "", nil
}

// DecodeMarkIndexMessage parses mark/index websocket payload.
func DecodeMarkIndexMessage(raw []byte) (*MarkIndexMessage, string, error) {
	var combined CombinedMarkIndexMessage
	if err := json.Unmarshal(raw, &combined); err == nil && combined.Data.Symbol != "" {
		return &combined.Data, combined.Stream, nil
	}

	var msg MarkIndexMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, "", err
	}
	return &msg, "", nil
}

// DecodeLiquidationMessage parses liquidation payload.
func DecodeLiquidationMessage(raw []byte) (*LiquidationMessage, string, error) {
	var combined CombinedLiquidationMessage
	if err := json.Unmarshal(raw, &combined); err == nil && combined.Data.Symbol != "" {
		return &combined.Data, combined.Stream, nil
	}

	var msg LiquidationMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, "", err
	}
	return &msg, "", nil
}
