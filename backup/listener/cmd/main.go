package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
	"github.com/sjtutwilight/Twilight/common/pkg/config"
	"github.com/sjtutwilight/Twilight/common/pkg/kafka"
	chain "github.com/sjtutwilight/Twilight/listener/pkg"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

var (
	configFile     = flag.String("config", "../../common/pkg/config/config.yaml", "Path to configuration file")
	deploymentFile = flag.String("deployment", "../../datasource/deployment.json", "Path to deployment configuration file")
)

func main() {
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Load deployment configuration
	deployment, err := config.LoadDeploymentConfig(*deploymentFile)
	if err != nil {
		log.Fatalf("Failed to load deployment configuration: %v", err)

	}

	// Create context that will be canceled on interrupt
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Create Kafka producer
	producerConfig := &kafka.ProducerConfig{
		Brokers:       cfg.Kafka.Brokers,
		Retries:       3,
		Timeout:       5000,
		BatchSize:     100,
		FlushMessages: 100,
		FlushBytes:    1024 * 1024,
		FlushInterval: 100,
	}
	producer, err := kafka.NewKafkaProducer(producerConfig, logrus.New(), cfg.Kafka.Topics.ChainTransactions, cfg.Kafka.Topics.ChainTransactions)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Create chain event listener
	client, err := ethclient.Dial(cfg.Chain.RPCURL)
	if err != nil {
		log.Fatalf("Failed to connect to Ethereum client: %v", err)
	}
	listener, err := chain.NewEventListener(client, uint64(cfg.Chain.ChainID))
	if err != nil {
		log.Fatalf("Failed to create event listener: %v", err)
	}

	// Add factory contract
	factoryAddr := common.HexToAddress(deployment.Factory)
	factoryContract := chain.NewContract(factoryAddr, "Factory")
	factoryContract.AddEventType("PairCreated", common.HexToHash("0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"))
	listener.AddContract(factoryAddr, factoryContract)

	// Add pair contracts
	for _, pair := range deployment.Pairs {
		pairAddr := common.HexToAddress(pair.Address)
		pairContract := chain.NewContract(pairAddr, "Pair")
		pairContract.AddEventType("Swap", common.HexToHash("0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"))
		pairContract.AddEventType("Mint", common.HexToHash("0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f"))
		pairContract.AddEventType("Burn", common.HexToHash("0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496"))
		pairContract.AddEventType("Sync", common.HexToHash("0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"))
		pairContract.AddEventType("Transfer", common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"))

		listener.AddContract(pairAddr, pairContract)
		log.Printf("Added pair contract at %s", pair.Address)
	}

	// Add token contracts
	for _, token := range deployment.Tokens {
		tokenAddr := common.HexToAddress(token.Address)
		tokenContract := chain.NewContract(tokenAddr, "Token")
		tokenContract.AddEventType("Transfer", common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"))
		tokenContract.AddEventType("Approval", common.HexToHash("0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"))
		listener.AddContract(tokenAddr, tokenContract)
		log.Printf("Added token contract at %s", token.Address)
	}

	// Create balance snapshot generator
	snapshotGenerator := chain.NewBalanceSnapshotGenerator(client, producer, "account_balance_snapshot", deployment)

	// Start balance snapshot generator
	go func() {
		if err := snapshotGenerator.Start(ctx); err != nil {
			log.Printf("Balance snapshot generator stopped: %v", err)
		}
	}()

	// Start listening for events
	go func() {
		if err := listener.Start(ctx); err != nil {
			log.Printf("Event listener stopped: %v", err)
			cancel()
		}
	}()

	// Process events
	go func() {
		eventChan := listener.GetEventChan()
		errorChan := listener.GetErrorChan()

		for {
			select {
			case event := <-eventChan:
				// Send event to Kafka using transaction hash as key
				// This ensures that messages with the same key go to the same partition
				// and can be deduplicated by consumers if needed

				// Convert event to JSON string
				eventJSON, err := json.Marshal(event)
				if err != nil {
					log.Printf("Failed to marshal event to JSON: %v", err)
					continue
				}

				if err := producer.SendMessage(ctx, cfg.Kafka.Topics.ChainTransactions, event.Transaction.TransactionHash, string(eventJSON)); err != nil {
					log.Printf("Failed to send event to Kafka: %v", err)
				} else {
					log.Printf("Successfully sent event for transaction %s to Kafka", event.Transaction.TransactionHash)
				}

			case err := <-errorChan:
				log.Printf("Chain event error: %v", err)

			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutting down gracefully...")
}
