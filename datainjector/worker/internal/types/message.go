package types

// Message 是内部传递的统一数据结构。
// Metadata 建议包含 chain_id、block_number 等字段。
type Message struct {
    Metadata map[string]any
    Payload  []byte
}

