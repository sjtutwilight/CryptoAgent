package chain

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// Contract represents a smart contract to monitor
type Contract struct {
	Address    common.Address
	Topics     []common.Hash
	Name       string
	EventTypes map[string]common.Hash
}

// NewContract creates a new contract instance
func NewContract(address common.Address, name string) *Contract {
	return &Contract{
		Address:    address,
		Name:       name,
		EventTypes: make(map[string]common.Hash),
	}
}

// AddEventType adds an event type to monitor
func (c *Contract) AddEventType(eventName string, eventSignature common.Hash) {
	c.EventTypes[eventName] = eventSignature
	c.Topics = append(c.Topics, eventSignature)
}

// IsInterestedInLog checks if the contract is interested in this log
func (c *Contract) IsInterestedInLog(log *types.Log) bool {
	if len(log.Topics) == 0 {
		return false
	}

	eventSignature := log.Topics[0]
	for _, topic := range c.Topics {
		if topic == eventSignature {
			return true
		}
	}
	return false
}

// GetEventName returns the name of the event based on its signature
func (c *Contract) GetEventName(eventSignature common.Hash) string {
	for name, signature := range c.EventTypes {
		if signature == eventSignature {
			return name
		}
	}
	return ""
}
