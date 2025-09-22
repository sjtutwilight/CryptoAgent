/**
 * Database Initialization Module
 * Handles initialization of database tables according to the updated schema
 * Only initializes tables that are updated by the scheduler (not Flink, microservices, or CQRS)
 */

const { ethers } = require('hardhat');
const { CHAIN_CONFIG } = require('../config');

/**
 * Initialize the database with token data
 * @param {Object} pgClient - PostgreSQL client
 * @param {Object} deployment - Deployment information
 */
async function initializeTokens(pgClient, deployment) {
  try {
    // Clear existing token data
    await pgClient.query('TRUNCATE TABLE token RESTART IDENTITY CASCADE');
    
    // Initialize token table with updated schema
    const tokens = deployment.tokens;
    for (const token of tokens) {
      // Get token category from config
      const tokenConfig = require('../config').TOKEN_CONFIG.find(t => t.symbol === token.symbol);
      const category = tokenConfig ? tokenConfig.category : 'unknown';
      
      // Set issuer based on token symbol
      let issuer = '';
      switch (token.symbol) {
        case 'USDC':
          issuer = 'Circle';
          break;
        case 'WETH':
          issuer = 'Ethereum';
          break;
        case 'DAI':
          issuer = 'MakerDAO';
          break;
        case 'TWI':
          issuer = 'yangguang';
          break;
        case 'WBTC':
          issuer = 'Bitcoin';
          break;
        default:
          issuer = 'Unknown';
      }
      
      const result = await pgClient.query(
        `INSERT INTO token (
          chain_id, chain_name, token_address, token_symbol, 
          token_decimals, token_catagory, issuer, create_time, update_time
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW()) RETURNING id`,
        [
          CHAIN_CONFIG.chainId, 
          CHAIN_CONFIG.chainName, 
          token.address, 
          token.symbol, 
          token.decimals,
          category,
          issuer
        ]
      );
      token.id = result.rows[0].id;
    }
    
    console.log('Successfully initialized token data');
    return tokens;
  } catch (error) {
    console.error('Error initializing tokens:', error);
    throw error;
  }
}

/**
 * Initialize the database with pair data
 * @param {Object} pgClient - PostgreSQL client
 * @param {Object} deployment - Deployment information
 * @param {Array} tokens - Token information with IDs
 */
async function initializePairs(pgClient, deployment, tokens) {
  try {
    // Clear existing pair data
    await pgClient.query('TRUNCATE TABLE twswap_pair RESTART IDENTITY CASCADE');
    
    // Initialize pair table
    const pairs = deployment.pairs;
    for (const pair of pairs) {
      const token0 = tokens.find(t => t.address.toLowerCase() === pair.token0.toLowerCase());
      const token1 = tokens.find(t => t.address.toLowerCase() === pair.token1.toLowerCase());

      if (!token0 || !token1) {
        console.warn(`Token not found for pair: ${pair.address}`);
        continue;
      }
      
      // Create pair name from token symbols
      const pairName = `${token0.symbol}-${token1.symbol}`;

      await pgClient.query(
        `INSERT INTO twswap_pair (
          chain_id, pair_address, pair_name, token0_id, token1_id, 
          fee_tier, created_at_timestamp, created_at_block_number
        ) VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7)`,
        [
          CHAIN_CONFIG.chainId, 
          pair.address, 
          pairName,
          token0.id, 
          token1.id, 
          '0.3%', 
          0
        ]
      );
    }
    
    console.log('Successfully initialized pair data');
  } catch (error) {
    console.error('Error initializing pairs:', error);
    throw error;
  }
}

/**
 * Initialize the database with account data
 * @param {Object} pgClient - PostgreSQL client
 * @param {Object} deployment - Deployment information
 */
async function initializeAccounts(pgClient, deployment) {
  try {
    // Import tag bitmap utilities
    const { tagStringToBitmap } = require('../utils/tagBitmap');
    
    // Clear existing account data
    await pgClient.query('TRUNCATE TABLE account RESTART IDENTITY CASCADE');
    
    // Initialize account table with bitmap tag system
    for (const accountInfo of deployment.accounts) {
      const address = accountInfo.address;
      
      // Convert tag string to bitmap value
      const tagBitmap = tagStringToBitmap(accountInfo.tag);
      
      console.log(`Initializing account ${address} with tag '${accountInfo.tag}' (bitmap: ${tagBitmap})`);
      
      await pgClient.query(
        `INSERT INTO account (
          chain_id, chain_name, address, entity, 
          tag_bitmap,
          create_time, update_time
        ) VALUES ($1, $2, $3, $4, $5, NOW(), NOW()) RETURNING id`,
        [
          CHAIN_CONFIG.chainId, 
          CHAIN_CONFIG.chainName, 
          address, 
          accountInfo.tag,
          tagBitmap
        ]
      );
    }
    
    console.log('Successfully initialized account data with bitmap tags');
  } catch (error) {
    console.error('Error initializing accounts:', error);
    throw error;
  }
}

/**
 * Print token metrics from database
 * @param {Object} pgClient - PostgreSQL client
 */
async function printTokenMetrics(pgClient) {
  try {
    console.log("\n=== TOKEN METRICS IN DATABASE ===");
    const result = await pgClient.query(`
      SELECT t.token_symbol, tm.token_price, tm.supply, tm.liquidity
      FROM token_metric tm
      JOIN token t ON tm.token_id = t.id
      ORDER BY t.token_symbol
    `);
    
    for (const row of result.rows) {
      console.log(`${row.token_symbol}: price=${row.token_price}, supply=${row.supply}, liquidity=${row.liquidity}`);
    }
    console.log("================================\n");
  } catch (error) {
    console.error("Error printing token metrics:", error);
  }
}

/**
 * Initialize token metrics (scheduler-managed table)
 * @param {Object} pgClient - PostgreSQL client
 * @param {Object} deployment - Deployment information
 * @param {Object} redis - Redis client for token prices
 */
async function initializeTokenMetrics(pgClient, deployment, redis) {
  try {
    console.log('Initializing token metrics...');
    
    // Clear existing token metrics data
    await pgClient.query('TRUNCATE TABLE token_metric RESTART IDENTITY CASCADE');
    
    // Get token configs for reference
    const tokenConfigs = require('../config').TOKEN_CONFIG;
    
    // Import Redis helper functions
    const { getTokenMetadata } = require('./redis');
    
    // Initialize token metrics
    for (const tokenInfo of deployment.tokens) {
      try {
        console.log(`Processing metrics for ${tokenInfo.symbol} (${tokenInfo.address})...`);
        
        const tokenContract = await ethers.getContractAt("MyERC20", tokenInfo.address);
        const totalSupply = await tokenContract.totalSupply();
        const tokenDecimals = parseInt(tokenInfo.decimals);
        const totalSupplyFormatted = ethers.formatUnits(totalSupply, tokenDecimals);
        
        // Get token metadata from Redis
        const tokenMetadata = await getTokenMetadata(redis, tokenInfo.address);
        
        // Get token price from Redis
        let price = '0';
        try {
          const redisKey = `token_price:${tokenInfo.address.toLowerCase()}`;
          console.log(`Getting price from Redis for key: ${redisKey}`);
          const redisPrice = await redis.get(redisKey);
          if (redisPrice) {
            price = redisPrice;
            console.log(`Found price in Redis for ${tokenInfo.symbol}: ${price}`);
          } else {
            console.warn(`No price found in Redis for ${tokenInfo.symbol}, checking config...`);
            
            // Fallback to config price
            const tokenConfig = tokenConfigs.find(t => t.symbol === tokenInfo.symbol);
            if (tokenConfig && tokenConfig.priceUSD) {
              price = tokenConfig.priceUSD.toString();
              console.log(`Using price from config for ${tokenInfo.symbol}: ${price}`);
            } else {
              console.warn(`No price found in config for ${tokenInfo.symbol}, using default price: 1`);
              price = '1';
            }
          }
        } catch (redisError) {
          console.warn(`Failed to get price from Redis for token ${tokenInfo.symbol}:`, redisError.message);
          price = '1';
        }
        
        // Calculate market cap and liquidity
        const mcap = parseFloat(totalSupplyFormatted) * parseFloat(price);
        const liquidity = mcap; // Using market cap as initial liquidity

        // Generate random security score and token age
        const securityScore = Math.floor(Math.random() * 100) + 1;
        const tokenAge = Math.floor(Math.random() * 365) + 1;
        
        console.log(`Metrics for ${tokenInfo.symbol}: price=${price}, mcap=${mcap}, liquidity=${liquidity}`);
        
        // Insert token metrics with new schema
        await pgClient.query(
          `INSERT INTO token_metric (
            token_id, token_price, token_age, liquidity_usd, security_score, fdv, mcap
          ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
          [
            tokenInfo.id,
            price,
            tokenAge,
            liquidity.toString(),
            securityScore,
            mcap.toString(), // FDV equals mcap for now
            mcap.toString()
          ]
        );
      } catch (error) {
        console.error(`Error initializing metrics for token ${tokenInfo.symbol}:`, error);
        // Continue with next token
      }
    }
    
    // Print token metrics with updated schema
    await pgClient.query(`
      SELECT t.token_symbol, tm.token_price, tm.liquidity_usd, tm.mcap
      FROM token_metric tm
      JOIN token t ON tm.token_id = t.id
      ORDER BY t.token_symbol
    `).then(result => {
      console.log("\n=== CURRENT TOKEN METRICS ===");
      for (const row of result.rows) {
        console.log(`${row.token_symbol}: price=${row.token_price}, liquidity=${row.liquidity_usd}, mcap=${row.mcap}`);
      }
      console.log("============================\n");
    }).catch(error => {
      console.error("Error printing token metrics:", error);
    });
    
    console.log('Successfully initialized token metrics');
  } catch (error) {
    console.error('Error initializing token metrics:', error);
    throw error;
  }
}

/**
 * Initialize token holder data (scheduler-managed table)
 * @param {Object} pgClient - PostgreSQL client
 * @param {Object} deployment - Deployment information
 * @param {Object} redis - Redis client for metadata
 */
async function initializeTokenHolders(pgClient, deployment, redis) {
  try {
    console.log('Initializing token holders...');
    
    // Clear existing token holder data
    await pgClient.query('TRUNCATE TABLE token_holder RESTART IDENTITY CASCADE');
    
    // Import Redis helper functions
    const { getTokenMetadata, getAccountMetadata } = require('./redis');
    
    // Process each token
    for (const tokenInfo of deployment.tokens) {
      try {
        console.log(`Processing holders for ${tokenInfo.symbol}...`);
        
        const tokenContract = await ethers.getContractAt("MyERC20", tokenInfo.address);
        const totalSupply = await tokenContract.totalSupply();
        const tokenDecimals = parseInt(tokenInfo.decimals);
        const totalSupplyFormatted = parseFloat(ethers.formatUnits(totalSupply, tokenDecimals));
        
        // Get token metadata from Redis
        const tokenMetadata = await getTokenMetadata(redis, tokenInfo.address);
        
        // Get token price for value_usd calculation
        let tokenPrice = 0;
        try {
          const redisKey = `token_price:${tokenInfo.address.toLowerCase()}`;
          const price = await redis.get(redisKey);
          if (price) {
            tokenPrice = parseFloat(price);
          }
        } catch (error) {
          console.warn(`Error getting price for token ${tokenInfo.symbol}:`, error.message);
        }
        
        // Get account result for mapping
        const accountResult = await pgClient.query('SELECT id, address, entity FROM account');
        const accountMap = new Map(accountResult.rows.map(row => [row.address.toLowerCase(), { id: row.id, entity: row.entity }]));
        
        let holderCount = 0;
        
        // Process each account
        for (const accountInfo of deployment.accounts) {
          const address = accountInfo.address;
          const accountData = accountMap.get(address.toLowerCase());
          
          if (!accountData) {
            console.warn(`Account data not found for address ${address}`);
            continue;
          }
          
          try {
            // Get token balance for this account
            const balance = await tokenContract.balanceOf(address);
            const balanceFormatted = parseFloat(ethers.formatUnits(balance, tokenDecimals));
            
            // Skip if balance is zero
            if (balanceFormatted === 0) {
              continue;
            }
            
            // Calculate ownership percentage
            const ownership = (balanceFormatted / totalSupplyFormatted) * 100;
            
            // Calculate value in USD
            const valueUsd = balanceFormatted * tokenPrice;
            
            // Skip if ownership is too small
            if (ownership < 0.01) {
              continue;
            }
            
            await pgClient.query(
              `INSERT INTO token_holder (
                token_id, account_id, token_address, account_address, 
                entity, value_usd, ownership
              ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
              [
                tokenInfo.id,
                accountData.id,
                tokenInfo.address,
                address,
                accountData.entity,
                valueUsd,
                ownership
              ]
            );
            
            console.log(`Added holder ${address} for ${tokenInfo.symbol} with ownership ${ownership.toFixed(2)}%`);
            holderCount++;
          } catch (error) {
            console.error(`Error processing holder ${address} for token ${tokenInfo.symbol}:`, error);
            // Continue with next account
          }
        }
        
        console.log(`Added ${holderCount} holders for ${tokenInfo.symbol}`);
      } catch (error) {
        console.error(`Error processing token ${tokenInfo.symbol}:`, error);
        // Continue with next token
      }
    }
    
    console.log('Successfully initialized token holders');
  } catch (error) {
    console.error('Error initializing token holders:', error);
    throw error;
  }
}

/**
 * Initialize native token assets for accounts (scheduler-managed)
 * @param {Object} pgClient - PostgreSQL client
 * @param {Object} deployment - Deployment information
 * @param {Object} redis - Redis client for metadata
 */
async function initializeNativeAssets(pgClient, deployment, redis) {
  try {
    console.log('Initializing native assets...');
    
    // Clear existing native asset data
    await pgClient.query(`
      DELETE FROM account_asset 
      WHERE asset_type = 'native'
    `);
    
    // Import Redis helper functions
    const { getAccountMetadata } = require('./redis');
    
    // Get account IDs from database
    const accountResult = await pgClient.query('SELECT id, address FROM account');
    const accountMap = new Map(accountResult.rows.map(row => [row.address.toLowerCase(), row.id]));
    
    // Initialize native assets for each account
    for (const accountInfo of deployment.accounts) {
      const address = accountInfo.address;
      const accountId = accountMap.get(address.toLowerCase());
      
      if (!accountId) {
        console.warn(`Account ID not found for address ${address}`);
        continue;
      }
      
      try {
        // Get account metadata from Redis
        const accountMetadata = await getAccountMetadata(redis, address);
        
        // Add native token (ETH) balance
        const ethBalance = await ethers.provider.getBalance(address);
        const ethBalanceFormatted = ethers.formatEther(ethBalance);
        
        console.log(`Setting native balance for account ${address}: ${ethBalanceFormatted} ETH`);
        
        // Prepare extended info with account metadata
        const extendInfo = {
          accountTag: accountMetadata ? accountMetadata.tag : null,
          chainId: deployment.chainId || require('../config').CHAIN_CONFIG.chainId,
          chainName: deployment.chainName || require('../config').CHAIN_CONFIG.chainName
        };
        
        await pgClient.query(
          `INSERT INTO account_asset (
            account_id, asset_type, biz_id, biz_name, value, extend_info, create_time, update_time
          ) VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
          ON CONFLICT (account_id, asset_type, biz_id) 
          DO UPDATE SET value = $5, update_time = NOW(), extend_info = $6`,
          [
            accountId,
            'native',
            1, // Default biz_id for native token
            'ETH',
            ethBalanceFormatted,
            JSON.stringify(extendInfo)
          ]
        );
      } catch (error) {
        console.error(`Error setting native balance for account ${address}:`, error);
        // Continue with next account instead of failing the entire process
      }
    }
    
    console.log('Successfully initialized native assets');
  } catch (error) {
    console.error('Error initializing native assets:', error);
    throw error;
  }
}

/**
 * Initialize ERC20 token assets for accounts (scheduler-managed)
 * @param {Object} pgClient - PostgreSQL client
 * @param {Object} deployment - Deployment information
 * @param {Object} redis - Redis client for metadata
 */
async function initializeERC20Assets(pgClient, deployment, redis) {
  try {
    console.log('Initializing ERC20 assets...');
    
    // Clear existing ERC20 asset data
    await pgClient.query(`
      DELETE FROM account_asset 
      WHERE asset_type = 'erc20'
    `);
    
    // Import Redis helper functions
    const { getTokenMetadata, getAccountMetadata } = require('./redis');
    
    // Get account IDs from database
    const accountResult = await pgClient.query('SELECT id, address FROM account');
    const accountMap = new Map(accountResult.rows.map(row => [row.address.toLowerCase(), row.id]));
    
    // Get token IDs from database
    const tokenResult = await pgClient.query('SELECT id, token_address, token_symbol FROM token');
    const tokenMap = new Map(tokenResult.rows.map(row => [row.token_address.toLowerCase(), { id: row.id, symbol: row.token_symbol }]));
    
    // Initialize ERC20 assets for each account
    for (const accountInfo of deployment.accounts) {
      const address = accountInfo.address;
      const accountId = accountMap.get(address.toLowerCase());
      
      if (!accountId) {
        console.warn(`Account ID not found for address ${address}`);
        continue;
      }
      
      // Get account metadata from Redis
      const accountMetadata = await getAccountMetadata(redis, address);
      
      // Process each token for this account
      for (const tokenInfo of deployment.tokens) {
        try {
          const tokenAddress = tokenInfo.address;
          const tokenData = tokenMap.get(tokenAddress.toLowerCase());
          
          if (!tokenData) {
            console.warn(`Token ID not found for address ${tokenAddress}`);
            continue;
          }
          
          // Get token metadata from Redis
          const tokenMetadata = await getTokenMetadata(redis, tokenAddress);
          
          // Get token balance for this account
          const tokenContract = await ethers.getContractAt("MyERC20", tokenAddress);
          const balance = await tokenContract.balanceOf(address);
          const tokenDecimals = parseInt(tokenInfo.decimals);
          const balanceFormatted = ethers.formatUnits(balance, tokenDecimals);
          
          // Skip if balance is zero
          if (parseFloat(balanceFormatted) === 0) {
            continue;
          }
          
          console.log(`Setting ERC20 balance for account ${address}: ${balanceFormatted} ${tokenInfo.symbol}`);
          
          // Prepare extended info with account and token metadata
          const extendInfo = {
            accountTag: accountMetadata ? accountMetadata.tag : null,
            tokenSymbol: tokenInfo.symbol,
            tokenDecimals: tokenDecimals,
            chainId: deployment.chainId || require('../config').CHAIN_CONFIG.chainId,
            chainName: deployment.chainName || require('../config').CHAIN_CONFIG.chainName
          };
          
          // Insert or update ERC20 asset
          await pgClient.query(
            `INSERT INTO account_asset (
              account_id, asset_type, biz_id, biz_name, value, extend_info, create_time, update_time
            ) VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
            ON CONFLICT (account_id, asset_type, biz_id) 
            DO UPDATE SET value = $5, update_time = NOW(), extend_info = $6`,
            [
              accountId,
              'erc20',
              tokenData.id, // biz_id is token_id for ERC20 assets
              tokenData.symbol,
              balanceFormatted,
              JSON.stringify(extendInfo)
            ]
          );
        } catch (error) {
          console.error(`Error setting ERC20 balance for account ${address} and token ${tokenInfo.symbol}:`, error);
          // Continue with next token instead of failing the entire process
        }
      }
    }
    
    console.log('Successfully initialized ERC20 assets');
  } catch (error) {
    console.error('Error initializing ERC20 assets:', error);
    throw error;
  }
}

/**
 * Initialize defiPosition assets for accounts (scheduler-managed)
 * @param {Object} pgClient - PostgreSQL client
 * @param {Object} deployment - Deployment information
 * @param {Object} redis - Redis client for metadata
 */
async function initializeDefiPositionAssets(pgClient, deployment, redis) {
  try {
    console.log('Initializing defiPosition assets...');
    
    // Clear existing defiPosition asset data
    await pgClient.query(`
      DELETE FROM account_asset 
      WHERE asset_type = 'defiPosition'
    `);
    
    // Import Redis helper functions
    const { getPairMetadata, getAccountMetadata } = require('./redis');
    
    // Get account IDs from database
    const accountResult = await pgClient.query('SELECT id, address FROM account');
    const accountMap = new Map(accountResult.rows.map(row => [row.address.toLowerCase(), row.id]));
    
    // Get pair IDs from database
    const pairResult = await pgClient.query(`
      SELECT p.id, p.pair_address, t0.token_symbol as token0_symbol, t1.token_symbol as token1_symbol
      FROM twswap_pair p
      JOIN token t0 ON p.token0_id = t0.id
      JOIN token t1 ON p.token1_id = t1.id
    `);
    
    const pairMap = new Map(pairResult.rows.map(row => [
      row.pair_address.toLowerCase(), 
      { 
        id: row.id, 
        token0Symbol: row.token0_symbol, 
        token1Symbol: row.token1_symbol 
      }
    ]));
    
    // Initialize defiPosition assets for each account
    for (const accountInfo of deployment.accounts) {
      const address = accountInfo.address;
      const accountId = accountMap.get(address.toLowerCase());
      
      if (!accountId) {
        console.warn(`Account ID not found for address ${address}`);
        continue;
      }
      
      // Get account metadata from Redis
      const accountMetadata = await getAccountMetadata(redis, address);
      
      // Process each pair for this account
      for (const pairInfo of deployment.pairs) {
        try {
          const pairAddress = pairInfo.address;
          const pairData = pairMap.get(pairAddress.toLowerCase());
          
          if (!pairData) {
            console.warn(`Pair ID not found for address ${pairAddress}`);
            continue;
          }
          
          // Get pair metadata from Redis
          const pairMetadata = await getPairMetadata(redis, pairAddress);
          
          // Get LP token balance for this account
          const pairContract = await ethers.getContractAt("TWSwapPair", pairAddress);
          const balance = await pairContract.balanceOf(address);
          const balanceFormatted = ethers.formatEther(balance); // LP tokens have 18 decimals
          
          // Skip if balance is zero
          if (parseFloat(balanceFormatted) === 0) {
            continue;
          }
          
          console.log(`Setting defiPosition balance for account ${address}: ${balanceFormatted} ${pairData.token0Symbol}-${pairData.token1Symbol} LP`);
          
          // Prepare extended info with account and pair metadata
          const extendInfo = {
            accountTag: accountMetadata ? accountMetadata.tag : null,
            token0Symbol: pairData.token0Symbol,
            token1Symbol: pairData.token1Symbol,
            chainId: deployment.chainId || require('../config').CHAIN_CONFIG.chainId,
            chainName: deployment.chainName || require('../config').CHAIN_CONFIG.chainName
          };
          
          // Insert or update defiPosition asset
          await pgClient.query(
            `INSERT INTO account_asset (
              account_id, asset_type, biz_id, biz_name, value, extend_info, create_time, update_time
            ) VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
            ON CONFLICT (account_id, asset_type, biz_id) 
            DO UPDATE SET value = $5, update_time = NOW(), extend_info = $6`,
            [
              accountId,
              'defiPosition',
              pairData.id, // biz_id is pair_id for defiPosition assets
              `${pairData.token0Symbol}-${pairData.token1Symbol}`,
              balanceFormatted,
              JSON.stringify(extendInfo)
            ]
          );
        } catch (error) {
          console.error(`Error setting defiPosition balance for account ${address} and pair ${pairInfo.address}:`, error);
          // Continue with next pair instead of failing the entire process
        }
      }
    }
    
    console.log('Successfully initialized defiPosition assets');
  } catch (error) {
    console.error('Error initializing defiPosition assets:', error);
    throw error;
  }
}

/**
 * Initialize all database tables that are managed by the scheduler
 * @param {Object} pgClient - PostgreSQL client
 * @param {Object} deployment - Deployment information
 * @param {Object} redis - Redis client for token prices
 */
async function initializeDatabase(pgClient, deployment, redis) {
  try {
    console.log('Starting database initialization for scheduler-managed tables...');
    
    // Initialize tokens first to get token IDs
    const tokens = await initializeTokens(pgClient, deployment);
    
    // Initialize pairs using token IDs
    await initializePairs(pgClient, deployment, tokens);
    
    // Initialize accounts
    await initializeAccounts(pgClient, deployment);
    
    // Initialize scheduler-managed tables
    
    // 1. Initialize token metrics (updated by scheduler)
    await initializeTokenMetrics(pgClient, deployment, redis);
    
    // 2. Initialize token holders (updated by scheduler)
    await initializeTokenHolders(pgClient, deployment, redis);
    
    // 3. Initialize native assets (updated by scheduler)
    await initializeNativeAssets(pgClient, deployment, redis);
    
    // 4. Initialize ERC20 assets (updated by scheduler)
    await initializeERC20Assets(pgClient, deployment, redis);
    
    // 5. Initialize defiPosition assets (updated by scheduler)
    await initializeDefiPositionAssets(pgClient, deployment, redis);
    
    console.log('Database initialization for scheduler-managed tables completed successfully');
  } catch (error) {
    console.error('Error during database initialization:', error);
    throw error;
  }
}

module.exports = {
  initializeDatabase,
  initializeTokens,
  initializePairs,
  initializeAccounts,
  initializeTokenMetrics,
  initializeTokenHolders,
  initializeNativeAssets,
  initializeERC20Assets,
  initializeDefiPositionAssets
}; 