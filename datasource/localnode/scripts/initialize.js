// scripts/initialize.js
const fs = require("fs");
const path = require("path");
const hre = require("hardhat");
const { ethers } = hre;
const { Client } = require('pg');
const Redis = require('ioredis');

// Import initialization modules
const { blockChainInit } = require('./init/blockchain');
const { initializeRedis, getTokenMetadata, getAccountMetadata, getPairMetadata } = require('./init/redis');
const { initializeDatabase } = require('./init/database');
const { startTokenMetricsUpdater, startAccountAssetUpdater, startTokenRollingMetricUpdater, startPriceUpdater } = require('./init/updaters');

/**
 * Main initialization function
 */
async function main() {
  try {
    // Deploy contracts, initialize accounts balance, pool liquidity, create deployment.json
    console.log("Starting blockchain initialization...");
    const deploymentInfo = await blockChainInit();
    
    // Wait for a few blocks to ensure all transactions are confirmed
    console.log("\nWaiting for transactions to be confirmed...");
    await new Promise(resolve => setTimeout(resolve, 2000)); // Wait for 2 seconds
    
    // Wait for file system to complete writing and reload deployment info
    console.log("Reloading deployment info...");
    delete require.cache[require.resolve('../../deployment.json')];
    const deployment = require('../../deployment.json');
    
    // Initialize Redis and PostgreSQL connections
    console.log("\nInitializing connections...");
    const redis = new Redis({
      host: 'localhost',
      port: 6379
    });
    const pgClient = new Client({
      host: 'localhost',
      port: 5432,
      database: 'twilight',
      user: 'twilight',
      password: 'twilight123'
    });
    await pgClient.connect();

    try {
      // Initialize Redis with token prices and metadata
      // Optimized to store token, account, and pair metadata in Redis for faster access
      console.log("\nInitializing Redis with token prices and metadata...");
      await initializeRedis(redis, deployment);

      // Start dynamic price updater first (before other updaters that depend on prices)
      console.log("\nStarting dynamic price updater...");
      await startPriceUpdater(deployment, redis, pgClient);
      console.log("Price updater started successfully");

      // Initialize database tables that are managed by the scheduler
      console.log("\nInitializing scheduler-managed database tables...");
      await initializeDatabase(pgClient, deployment, redis);
      
      // Start periodic token metrics updates (scheduler task)
      console.log("\nStarting periodic token metrics updates...");
      await startTokenMetricsUpdater(deployment, redis, pgClient);
      console.log("Token metrics updater started successfully");
      
      // Start periodic account asset balance updates for native tokens (scheduler task)
      console.log("\nStarting periodic native asset balance updates...");
      await startAccountAssetUpdater(deployment, redis, pgClient);
      console.log("Native asset updater started successfully");
      
      // Start periodic token rolling metric updates (scheduler task)
      console.log("\nStarting periodic token rolling metric updates...");
      await startTokenRollingMetricUpdater(deployment, redis, pgClient);
      console.log("Token rolling metric updater started successfully");
      
      console.log("\nNote: Other tables (ERC20 assets, defiPosition assets, views, etc.) will be updated by microservices, Flink, or CQRS processes");
      
      // Keep the process running
      process.stdin.resume();
      
      // Handle graceful shutdown
      process.on('SIGINT', async () => {
        console.log('\nReceived SIGINT. Shutting down gracefully...');
        await redis.quit();
        await pgClient.end();
        process.exit(0);
      });

      return deploymentInfo;
    } catch (error) {
      console.error('Error during initialization:', error);
      await redis.quit();
      await pgClient.end();
      process.exit(1);
    }
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

// We recommend this pattern to be able to use async/await everywhere
// and properly handle errors.
main()
  .then(() => {
    console.log("Deployment and initialization complete");
  })
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });