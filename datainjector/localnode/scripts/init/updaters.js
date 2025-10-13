/**
 * Periodic Updaters Module
 * Handles periodic updates of token metrics and account assets
 * Only updates tables that are managed by the scheduler according to the data flow diagram
 */

const { ethers } = require('hardhat');
const { CHAIN_CONFIG } = require('../config');
const { getAccountMetadata, getTokenMetadata, getPairMetadata } = require('./redis');

// ERC20 ABI for interacting with token contracts
const ERC20_ABI = [
  // Read-only functions
  "function name() view returns (string)",
  "function symbol() view returns (string)",
  "function decimals() view returns (uint8)",
  "function totalSupply() view returns (uint256)",
  "function balanceOf(address owner) view returns (uint256)",
  "function allowance(address owner, address spender) view returns (uint256)",
  
  // Write functions
  "function transfer(address to, uint256 value) returns (bool)",
  "function approve(address spender, uint256 value) returns (bool)",
  "function transferFrom(address from, address to, uint256 value) returns (bool)",
  
  // Events
  "event Transfer(address indexed from, address indexed to, uint256 value)",
  "event Approval(address indexed owner, address indexed spender, uint256 value)"
];

// TWSwapPair ABI for interacting with pair contracts
const PAIR_ABI = [
  // Read-only functions
  "function balanceOf(address owner) view returns (uint256)",
  "function token0() view returns (address)",
  "function token1() view returns (address)",
  "function getReserves() view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)",
  "function totalSupply() view returns (uint256)"
];

/**
 * Start periodic token metrics updater
 * @param {Object} deployment - Deployment information
 * @param {Object} redis - Redis client
 * @param {Object} pgClient - PostgreSQL client
 */
async function startTokenMetricsUpdater(deployment, redis, pgClient) {
  const updateTokenMetrics = async () => {
    try {
      console.log('Updating token metrics...');
      
      // Get token configs for reference
      const tokenConfigs = require('../config').TOKEN_CONFIG;
      
      // Get token IDs from database
      const tokenResult = await pgClient.query('SELECT id, token_address, token_decimals, token_symbol FROM token');
      const tokenMap = new Map(tokenResult.rows.map(row => [row.token_address.toLowerCase(), row]));
      
      // Get pair data for liquidity calculation
      const pairResult = await pgClient.query(`
        SELECT p.id, p.pair_address, t0.token_address as token0_address, t1.token_address as token1_address, 
               t0.token_symbol as token0_symbol, t1.token_symbol as token1_symbol
        FROM twswap_pair p
        JOIN token t0 ON p.token0_id = t0.id
        JOIN token t1 ON p.token1_id = t1.id
      `);
      
      // Create token to pairs mapping
      const tokenToPairs = new Map();
      for (const pair of pairResult.rows) {
        const token0Address = pair.token0_address.toLowerCase();
        const token1Address = pair.token1_address.toLowerCase();
        
        if (!tokenToPairs.has(token0Address)) {
          tokenToPairs.set(token0Address, []);
        }
        if (!tokenToPairs.has(token1Address)) {
          tokenToPairs.set(token1Address, []);
        }
        
        tokenToPairs.get(token0Address).push(pair);
        tokenToPairs.get(token1Address).push(pair);
      }
      
      // Update metrics for each token
      for (const tokenInfo of deployment.tokens) {
        const address = tokenInfo.address.toLowerCase();
        const tokenData = tokenMap.get(address);
        
        if (!tokenData) {
          console.warn(`Token data not found for address ${address}`);
          continue;
        }
        
        try {
          const tokenId = tokenData.id;
          const tokenContract = new ethers.Contract(tokenInfo.address, ERC20_ABI, ethers.provider);
          
          // Get token metrics
          const totalSupply = await tokenContract.totalSupply();
          const totalSupplyFormatted = ethers.formatUnits(totalSupply, tokenData.token_decimals);
          
          // Get token price from Redis
          let price = '0';
          try {
            const redisKey = `token_price:${address}`;
            console.log(`Getting price from Redis for key: ${redisKey}`);
            const redisPrice = await redis.get(redisKey);
            if (redisPrice) {
              price = redisPrice;
              console.log(`Found price in Redis for ${tokenData.token_symbol}: ${price}`);
            } else {
              console.warn(`No price found in Redis for ${tokenData.token_symbol}, checking config...`);
              
              // Fallback to config price
              const tokenConfig = tokenConfigs.find(t => t.symbol === tokenData.token_symbol);
              if (tokenConfig && tokenConfig.priceUSD) {
                price = tokenConfig.priceUSD.toString();
                console.log(`Using price from config for ${tokenData.token_symbol}: ${price}`);
              } else {
                console.warn(`No price found in config for ${tokenData.token_symbol}, using default price: 1`);
                price = '1';
              }
            }
          } catch (redisError) {
            console.warn(`Failed to get price from Redis for token ${tokenData.token_symbol}:`, redisError.message);
            price = '1';
          }
          
          // Calculate market cap
          const mcap = parseFloat(totalSupplyFormatted) * parseFloat(price);
          const fdv = mcap; // Fully diluted valuation equals market cap for now
          
          // Calculate liquidity from pairs
          let liquidityUsd = 0;
          const pairs = tokenToPairs.get(address) || [];
          
          for (const pair of pairs) {
            try {
              const pairContract = new ethers.Contract(
                pair.pair_address,
                PAIR_ABI,
                ethers.provider
              );
              
              const reserves = await pairContract.getReserves();
              const reserve0 = ethers.formatUnits(reserves[0], 18); // Assuming 18 decimals
              const reserve1 = ethers.formatUnits(reserves[1], 18); // Assuming 18 decimals
              
              // Get token prices
              let token0Price = 1, token1Price = 1;
              
              // Try to get prices from Redis
              try {
                const token0PriceKey = `token_price:${pair.token0_address.toLowerCase()}`;
                const token1PriceKey = `token_price:${pair.token1_address.toLowerCase()}`;
                
                const token0PriceRedis = await redis.get(token0PriceKey);
                const token1PriceRedis = await redis.get(token1PriceKey);
                
                if (token0PriceRedis) token0Price = parseFloat(token0PriceRedis);
                if (token1PriceRedis) token1Price = parseFloat(token1PriceRedis);
                
                // If prices not found in Redis, use defaults based on symbol
                if (!token0PriceRedis) {
                  if (pair.token0_symbol === 'USDC' || pair.token0_symbol === 'DAI') token0Price = 1;
                  else if (pair.token0_symbol === 'WETH') token0Price = 3000;
                  else if (pair.token0_symbol === 'TWI') token0Price = 50;
                  else if (pair.token0_symbol === 'WBTC') token0Price = 120000;
                }
                
                if (!token1PriceRedis) {
                  if (pair.token1_symbol === 'USDC' || pair.token1_symbol === 'DAI') token1Price = 1;
                  else if (pair.token1_symbol === 'WETH') token1Price = 3000;
                  else if (pair.token1_symbol === 'TWI') token1Price = 50;
                  else if (pair.token1_symbol === 'WBTC') token1Price = 120000;
                }
              } catch (error) {
                console.warn(`Error getting prices for pair ${pair.pair_address}:`, error.message);
              }
              
              // Calculate liquidity value
              const reserve0Value = parseFloat(reserve0) * token0Price;
              const reserve1Value = parseFloat(reserve1) * token1Price;
              const pairLiquidity = reserve0Value + reserve1Value;
              
              // If this token is in the pair, add half of the pair's liquidity
              liquidityUsd += pairLiquidity / 2;
              
              console.log(`Pair ${pair.token0_symbol}-${pair.token1_symbol} liquidity: $${pairLiquidity}`);
            } catch (error) {
              console.error(`Error calculating liquidity for pair ${pair.pair_address}:`, error);
            }
          }
          
          // If no liquidity found in pairs, use a percentage of market cap
          if (liquidityUsd === 0) {
            if (tokenData.token_symbol === 'USDC' || tokenData.token_symbol === 'DAI') {
              liquidityUsd = mcap * 0.8; // Stablecoins tend to have higher liquidity
            } else if (tokenData.token_symbol === 'WETH') {
              liquidityUsd = mcap * 0.7; // ETH has high liquidity
            } else {
              liquidityUsd = mcap * 0.5; // Default to 50% of mcap
            }
          }

          // Get token age - use real data or increment existing
          let tokenAge;
          const existingMetrics = await pgClient.query(
            'SELECT token_age FROM token_metric WHERE token_id = $1',
            [tokenId]
          );
          
          if (existingMetrics.rowCount > 0) {
            // Increment age by 1 day if updating
            tokenAge = existingMetrics.rows[0].token_age + 1;
          } else {
            // For new tokens, estimate age based on deployment info or use default
            tokenAge = 1; // Start with 1 day for new tokens
          }
          
          // Security score - use a deterministic calculation based on token properties
          // Higher liquidity and age = higher security
          const liquidityFactor = Math.min(1, liquidityUsd / 1000000) * 50; // Up to 50 points for liquidity
          const ageFactor = Math.min(1, tokenAge / 365) * 50; // Up to 50 points for age
          const securityScore = Math.floor(liquidityFactor + ageFactor);
          
          console.log(`Metrics for ${tokenData.token_symbol}: price=${price}, mcap=${mcap}, liquidity=${liquidityUsd}, security=${securityScore}, age=${tokenAge}`);
          
          // Update token metrics with new schema
          await pgClient.query(
            `INSERT INTO token_metric (
              token_id, token_price, token_age, liquidity_usd, security_score, fdv, mcap,
              create_time, update_time
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
            ON CONFLICT (token_id) DO UPDATE SET
              token_price = $2,
              token_age = $3,
              liquidity_usd = $4,
              security_score = $5,
              fdv = $6,
              mcap = $7,
              update_time = NOW()`,
            [
              tokenId,
              price,
              tokenAge,
              liquidityUsd.toString(),
              securityScore,
              fdv.toString(),
              mcap.toString()
            ]
          );
          
          // Update token holders
          await updateTokenHolders(tokenId, tokenContract, tokenData.token_decimals, totalSupplyFormatted, pgClient);
          
        } catch (error) {
          console.error(`Error updating metrics for token ${tokenData.token_symbol}:`, error);
          // Continue with next token
        }
      }
      
      // Print token metrics with updated schema
      await pgClient.query(`
        SELECT t.token_symbol, tm.token_price, tm.liquidity_usd, tm.mcap, tm.token_age, tm.security_score, 
               tm.create_time, tm.update_time
        FROM token_metric tm
        JOIN token t ON tm.token_id = t.id
        ORDER BY t.token_symbol
      `).then(result => {
        console.log("\n=== CURRENT TOKEN METRICS ===");
        for (const row of result.rows) {
          console.log(`${row.token_symbol}: price=${row.token_price}, liquidity=${row.liquidity_usd}, mcap=${row.mcap}, age=${row.token_age}, security=${row.security_score}`);
          console.log(`  created: ${row.create_time}, updated: ${row.update_time}`);
        }
        console.log("============================\n");
      }).catch(error => {
        console.error("Error printing token metrics:", error);
      });
      
      console.log('Token metrics update completed');
    } catch (error) {
      console.error('Error updating token metrics:', error);
    }
  };
  
  // Run initial update
  await updateTokenMetrics();
  
  // Schedule updates every minute
  setInterval(updateTokenMetrics, 60000);
  
  console.log('Token metrics updater started');
}

/**
 * Update token holders for a specific token
 * @param {number} tokenId - Token ID in database
 * @param {Object} tokenContract - Ethers.js contract instance
 * @param {number} tokenDecimals - Token decimals
 * @param {string} totalSupply - Total supply of the token
 * @param {Object} pgClient - PostgreSQL client
 */
async function updateTokenHolders(tokenId, tokenContract, tokenDecimals, totalSupply, pgClient) {
  try {
    // Get all accounts
    const accountResult = await pgClient.query('SELECT id, address, entity FROM account');
    const accounts = accountResult.rows;
    
    // Get token price from token_metric table
    const tokenPriceResult = await pgClient.query(
      'SELECT token_price FROM token_metric WHERE token_id = $1',
      [tokenId]
    );
    
    let tokenPrice = 1; // Default price
    if (tokenPriceResult.rowCount > 0) {
      tokenPrice = parseFloat(tokenPriceResult.rows[0].token_price);
    }
    
    // Get token address
    const tokenAddressResult = await pgClient.query(
      'SELECT token_address FROM token WHERE id = $1',
      [tokenId]
    );
    
    let tokenAddress = '';
    if (tokenAddressResult.rowCount > 0) {
      tokenAddress = tokenAddressResult.rows[0].token_address;
    } else {
      tokenAddress = tokenContract.address;
    }
    
    // Process each account
    for (const account of accounts) {
      try {
        const balance = await tokenContract.balanceOf(account.address);
        const balanceFormatted = ethers.formatUnits(balance, tokenDecimals);
        
        // Skip zero balances
        if (parseFloat(balanceFormatted) <= 0) {
          continue;
        }
        
        // Calculate ownership percentage
        const ownership = (parseFloat(balanceFormatted) / parseFloat(totalSupply)) * 100;
        
        // Calculate USD value
        const valueUsd = parseFloat(balanceFormatted) * tokenPrice;
        
        console.log(`Updating token holder: account=${account.address}, token=${tokenAddress}, balance=${balanceFormatted}, valueUsd=${valueUsd}, ownership=${ownership}%`);
        
        // Update token_holder table
        await pgClient.query(
          `INSERT INTO token_holder (
            token_id, account_id, token_address, account_address, 
            entity, value_usd, ownership,
            create_time, update_time
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
          ON CONFLICT (token_id, account_id) DO UPDATE SET
            value_usd = $6,
            ownership = $7,
            update_time = NOW()`,
          [
            tokenId,
            account.id,
            tokenAddress,
            account.address,
            account.entity,
            valueUsd,
            ownership
          ]
        );
        
        // Verify the update was successful
        const verifyResult = await pgClient.query(
          `SELECT update_time FROM token_holder WHERE token_id = $1 AND account_id = $2`,
          [tokenId, account.id]
        );
        
        if (verifyResult.rowCount > 0) {
          console.log(`Verified token holder update: update_time = ${verifyResult.rows[0].update_time}`);
        } else {
          console.warn(`Failed to verify token holder update for token_id=${tokenId}, account_id=${account.id}`);
        }
      } catch (error) {
        console.error(`Error processing holder ${account.address}:`, error);
      }
    }
  } catch (error) {
    console.error(`Error updating token holders for token ID ${tokenId}:`, error);
  }
}

/**
 * Start periodic account asset updater for all asset types
 * @param {Object} deployment - Deployment information
 * @param {Object} redis - Redis client
 * @param {Object} pgClient - PostgreSQL client
 */
async function startAccountAssetUpdater(deployment, redis, pgClient) {
  const updateAccountAssets = async () => {
    try {
      console.log('Updating account assets...');
      
      // Get account IDs from database
      const accountResult = await pgClient.query('SELECT id, address FROM account');
      const accountMap = new Map(accountResult.rows.map(row => [row.address.toLowerCase(), row.id]));
      
      // Import Redis helper functions
      const { getTokenMetadata, getPairMetadata } = require('./redis');
      
      // 1. Update native assets for each account (scheduler-managed)
      await updateNativeAssets(deployment, redis, pgClient, accountMap);
      
      // 2. Update ERC20 assets for each account (scheduler-managed)
      await updateERC20Assets(deployment, redis, pgClient, accountMap);
      
      // 3. Update defiPosition assets for each account (scheduler-managed)
      await updateDefiPositionAssets(deployment, redis, pgClient, accountMap);
      
      console.log('Account assets update completed');
    } catch (error) {
      console.error('Error updating account assets:', error);
    }
  };
  
  // Run initial update
  await updateAccountAssets();
  
  // Schedule updates every minute
  setInterval(updateAccountAssets, 60000);
  
  console.log('Account asset updater started');
}

/**
 * Update native assets for accounts
 * @param {Object} deployment - Deployment information
 * @param {Object} redis - Redis client
 * @param {Object} pgClient - PostgreSQL client
 * @param {Map} accountMap - Map of account addresses to IDs
 */
async function updateNativeAssets(deployment, redis, pgClient, accountMap) {
  try {
    console.log('Updating native account assets...');
    
    // Update native assets for each account (scheduler-managed)
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
        
        // Update native token (ETH) balance
        const ethBalance = await ethers.provider.getBalance(address);
        const ethBalanceFormatted = ethers.formatEther(ethBalance);
        
        // Get ETH price from WETH token price
        let ethPrice = 3000; // Default to 3000 if not found
        try {
          // Find WETH token info
          const wethToken = deployment.tokens.find(t => t.symbol === 'WETH');
          if (wethToken) {
            // First check token_metric table
            const wethPriceResult = await pgClient.query(
              `SELECT tm.token_price 
               FROM token_metric tm
               JOIN token t ON tm.token_id = t.id
               WHERE t.token_address = $1`,
              [wethToken.address]
            );
            
            if (wethPriceResult.rowCount > 0) {
              ethPrice = parseFloat(wethPriceResult.rows[0].token_price);
              console.log(`Using WETH price from token_metric: ${ethPrice}`);
            } else {
              // Fallback to Redis
              const wethMetadata = await getTokenMetadata(redis, wethToken.address);
              if (wethMetadata && wethMetadata.priceUsd) {
                ethPrice = parseFloat(wethMetadata.priceUsd);
                console.log(`Using WETH price from Redis: ${ethPrice}`);
              } else {
                console.log(`Using default WETH price: ${ethPrice}`);
              }
            }
          }
        } catch (error) {
          console.warn(`Error getting ETH price: ${error.message}`);
        }
        
        // Calculate USD value
        const valueUsd = parseFloat(ethBalanceFormatted) * ethPrice;
        
        console.log(`Processing native balance for account ${address}: ${ethBalanceFormatted} ETH (${valueUsd} USD) at price ${ethPrice}`);
        
        // Check if the account asset exists
        const existingAsset = await pgClient.query(
          `SELECT account_id FROM account_asset_view 
           WHERE account_id = $1 AND asset_type = 'native' AND biz_id = 1`,
          [accountId]
        );
        
        if (existingAsset.rowCount === 0) {
          // Insert if not exists
          await pgClient.query(
            `INSERT INTO account_asset_view (
              account_id, asset_type, biz_id, biz_name, value, value_usd, asset_price,
              create_time, update_time
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())`,
            [
              accountId,
              'native',
              1, // Default biz_id for native token
              'ETH',
              ethBalanceFormatted,
              valueUsd,
              ethPrice
            ]
          );
          console.log(`Inserted native balance for account ${address}: ${ethBalanceFormatted} ETH (${valueUsd} USD)`);
        } else {
          // Update if exists
          await pgClient.query(
            `UPDATE account_asset_view 
             SET value = $1, value_usd = $2, asset_price = $3, update_time = NOW()
             WHERE account_id = $4 AND asset_type = 'native' AND biz_id = 1`,
            [ethBalanceFormatted, valueUsd, ethPrice, accountId]
          );
          console.log(`Updated native balance for account ${address}: ${ethBalanceFormatted} ETH (${valueUsd} USD)`);
        }
        
        // Verify the update was successful
        const verifyResult = await pgClient.query(
          `SELECT update_time FROM account_asset_view 
           WHERE account_id = $1 AND asset_type = 'native' AND biz_id = 1`,
          [accountId]
        );
        
        if (verifyResult.rowCount > 0) {
          console.log(`Verified native asset update: update_time = ${verifyResult.rows[0].update_time}`);
        } else {
          console.warn(`Failed to verify native asset update for account_id=${accountId}`);
        }
      } catch (error) {
        console.error(`Error updating native balance for account ${address}:`, error);
        // Continue with next account instead of failing the entire process
      }
    }
    
    console.log('Native account assets update completed');
  } catch (error) {
    console.error('Error updating native assets:', error);
  }
}

/**
 * Update ERC20 assets for accounts
 * @param {Object} deployment - Deployment information
 * @param {Object} redis - Redis client
 * @param {Object} pgClient - PostgreSQL client
 * @param {Map} accountMap - Map of account addresses to IDs
 */
async function updateERC20Assets(deployment, redis, pgClient, accountMap) {
  try {
    console.log('Updating ERC20 account assets...');
    
    // Get token IDs from database
    const tokenResult = await pgClient.query('SELECT id, token_address, token_symbol, token_decimals FROM token');
    const tokenMap = new Map(tokenResult.rows.map(row => [
      row.token_address.toLowerCase(), 
      { 
        id: row.id, 
        symbol: row.token_symbol, 
        decimals: row.token_decimals 
      }
    ]));
    
    // Get current token prices from token_metric table
    const tokenPricesResult = await pgClient.query('SELECT token_id, token_price FROM token_metric');
    const tokenPriceMap = new Map(tokenPricesResult.rows.map(row => [row.token_id, parseFloat(row.token_price)]));
    
    // Update ERC20 assets for each account
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
          const tokenAddress = tokenInfo.address.toLowerCase();
          const tokenData = tokenMap.get(tokenAddress);
          
          if (!tokenData) {
            console.warn(`Token data not found for address ${tokenAddress}`);
            continue;
          }
          
          // Get token balance for this account
          const tokenContract = new ethers.Contract(tokenInfo.address, ERC20_ABI, ethers.provider);
          const balance = await tokenContract.balanceOf(address);
          const balanceFormatted = ethers.formatUnits(balance, tokenData.decimals);
          
          // Skip if balance is zero
          if (parseFloat(balanceFormatted) === 0) {
            continue;
          }
          
          // Get token price from token_metric table
          let tokenPrice = tokenPriceMap.get(tokenData.id) || 0;
          
          // If price not found in token_metric, use fallback
          if (!tokenPrice) {
            try {
              const tokenMetadata = await getTokenMetadata(redis, tokenInfo.address);
              if (tokenMetadata && tokenMetadata.priceUsd) {
                tokenPrice = parseFloat(tokenMetadata.priceUsd);
              } else {
                // Fallback to default prices based on token symbol
                switch (tokenData.symbol) {
                  case 'USDC':
                  case 'DAI':
                    tokenPrice = 1;
                    break;
                  case 'WETH':
                    tokenPrice = 3000;
                    break;
                  case 'TWI':
                    tokenPrice = 50;
                    break;
                  case 'WBTC':
                    tokenPrice = 120000;
                    break;
                  default:
                    tokenPrice = 0;
                }
              }
            } catch (error) {
              console.warn(`Error getting price for token ${tokenData.symbol}: ${error.message}`);
              // Use default prices
              switch (tokenData.symbol) {
                case 'USDC':
                case 'DAI':
                  tokenPrice = 1;
                  break;
                case 'WETH':
                  tokenPrice = 3000;
                  break;
                case 'TWI':
                  tokenPrice = 50;
                  break;
                case 'WBTC':
                  tokenPrice = 120000;
                  break;
                default:
                  tokenPrice = 0;
              }
            }
          }
          
          // Calculate USD value
          const valueUsd = parseFloat(balanceFormatted) * tokenPrice;
          
          console.log(`Processing ERC20 balance for account ${address}: ${balanceFormatted} ${tokenData.symbol} (${valueUsd} USD) at price ${tokenPrice}`);
          
          // Check if the account asset exists
          const existingAsset = await pgClient.query(
            `SELECT account_id FROM account_asset_view 
             WHERE account_id = $1 AND asset_type = 'erc20' AND biz_id = $2`,
            [accountId, tokenData.id]
          );
          
          if (existingAsset.rowCount === 0) {
            // Insert if not exists
            await pgClient.query(
              `INSERT INTO account_asset_view (
                account_id, asset_type, biz_id, biz_name, value, value_usd, asset_price,
                create_time, update_time
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())`,
              [
                accountId,
                'erc20',
                tokenData.id, // biz_id is token_id for ERC20 assets
                tokenData.symbol,
                balanceFormatted,
                valueUsd,
                tokenPrice
              ]
            );
            console.log(`Inserted ERC20 balance for account ${address}: ${balanceFormatted} ${tokenData.symbol} (${valueUsd} USD)`);
          } else {
            // Update if exists
            await pgClient.query(
              `UPDATE account_asset_view 
               SET value = $1, value_usd = $2, asset_price = $3, update_time = NOW()
               WHERE account_id = $4 AND asset_type = 'erc20' AND biz_id = $5`,
              [balanceFormatted, valueUsd, tokenPrice, accountId, tokenData.id]
            );
            console.log(`Updated ERC20 balance for account ${address}: ${balanceFormatted} ${tokenData.symbol} (${valueUsd} USD)`);
          }
          
          // Verify the update was successful
          const verifyResult = await pgClient.query(
            `SELECT update_time FROM account_asset_view 
             WHERE account_id = $1 AND asset_type = 'erc20' AND biz_id = $2`,
            [accountId, tokenData.id]
          );
          
          if (verifyResult.rowCount > 0) {
            console.log(`Verified ERC20 asset update: update_time = ${verifyResult.rows[0].update_time}`);
          } else {
            console.warn(`Failed to verify ERC20 asset update for account_id=${accountId}, token=${tokenData.symbol}`);
          }
        } catch (error) {
          console.error(`Error updating ERC20 balance for account ${address}:`, error);
          // Continue with next token instead of failing the entire process
        }
      }
    }
    
    console.log('ERC20 account assets update completed');
  } catch (error) {
    console.error('Error updating ERC20 assets:', error);
  }
}

/**
 * Update defiPosition assets for accounts
 * @param {Object} deployment - Deployment information
 * @param {Object} redis - Redis client
 * @param {Object} pgClient - PostgreSQL client
 * @param {Map} accountMap - Map of account addresses to IDs
 */
async function updateDefiPositionAssets(deployment, redis, pgClient, accountMap) {
  console.log("Updating defiPosition account assets...");
  
  // Get pair IDs from database
  const pairResult = await pgClient.query(`
    SELECT p.id, p.pair_address as address, t1.token_symbol as token0_symbol, t2.token_symbol as token1_symbol,
           t1.id as token0_id, t2.id as token1_id
    FROM twswap_pair p
    JOIN token t1 ON p.token0_id = t1.id
    JOIN token t2 ON p.token1_id = t2.id
  `);
  
  // Create a map of pair address to pair data
  const pairMap = new Map();
  for (const pair of pairResult.rows) {
    pairMap.set(pair.address.toLowerCase(), {
      id: pair.id,
      address: pair.address.toLowerCase(),
      token0Symbol: pair.token0_symbol,
      token1Symbol: pair.token1_symbol,
      token0Id: pair.token0_id,
      token1Id: pair.token1_id
    });
  }
  
  // Get token prices from token_metric table
  const tokenPricesResult = await pgClient.query('SELECT token_id, token_price FROM token_metric');
  const tokenPriceMap = new Map(tokenPricesResult.rows.map(row => [row.token_id, parseFloat(row.token_price)]));
  
  // Process each account
  for (const account of deployment.accounts) {
    const accountId = accountMap.get(account.address.toLowerCase());
    if (!accountId) {
      console.warn(`Could not find account ID for ${account.address}`);
      continue;
    }
    
    // Get account metadata
    const accountMetadata = await getAccountMetadata(redis, account.address);
    const address = account.address;
    
    // Process each pair
    for (const pair of deployment.pairs) {
      const pairAddress = pair.address.toLowerCase();
      const pairData = pairMap.get(pairAddress);
      
      if (!pairData) {
        console.warn(`Could not find pair data for ${pairAddress}`);
        continue;
      }
      
      // Get pair contract
      const pairContract = new ethers.Contract(
        pairAddress,
        PAIR_ABI,
        ethers.provider
      );
      
      try {
        // Check LP token balance
        const balance = await pairContract.balanceOf(address);
        // Handle both ethers v5 and v6 format
        const balanceFormatted = ethers.utils?.formatEther 
          ? ethers.utils.formatEther(balance) 
          : ethers.formatEther(balance);
        
        // Skip if balance is zero
        if (parseFloat(balanceFormatted) === 0) {
          continue;
        }
        
        // Get reserves and calculate liquidity
        const reserves = await pairContract.getReserves();
        const reserve0 = ethers.utils?.formatEther 
          ? ethers.utils.formatEther(reserves[0]) 
          : ethers.formatEther(reserves[0]);
        const reserve1 = ethers.utils?.formatEther 
          ? ethers.utils.formatEther(reserves[1]) 
          : ethers.formatEther(reserves[1]);
        
        // Get total supply
        const totalSupply = await pairContract.totalSupply();
        const totalSupplyFormatted = ethers.utils?.formatEther 
          ? ethers.utils.formatEther(totalSupply) 
          : ethers.formatEther(totalSupply);
        
        // Get token prices from token_metric table
        let token0Price = tokenPriceMap.get(pairData.token0Id) || 0;
        let token1Price = tokenPriceMap.get(pairData.token1Id) || 0;
        
        // If prices not found in token_metric, try Redis or use defaults
        if (!token0Price || !token1Price) {
          try {
            // Get token addresses
            const token0Address = await pairContract.token0();
            const token1Address = await pairContract.token1();
            
            // Try to get prices from Redis
            if (!token0Price) {
              const token0Metadata = await getTokenMetadata(redis, token0Address);
              if (token0Metadata && token0Metadata.priceUsd) {
                token0Price = parseFloat(token0Metadata.priceUsd);
              } else {
                // Fallback to default prices based on token symbol
                if (pairData.token0Symbol === 'USDC' || pairData.token0Symbol === 'DAI') token0Price = 1;
                else if (pairData.token0Symbol === 'WETH') token0Price = 3000;
                else if (pairData.token0Symbol === 'TWI') token0Price = 50;
                else if (pairData.token0Symbol === 'WBTC') token0Price = 120000;
                else token0Price = 1;
              }
            }
            
            if (!token1Price) {
              const token1Metadata = await getTokenMetadata(redis, token1Address);
              if (token1Metadata && token1Metadata.priceUsd) {
                token1Price = parseFloat(token1Metadata.priceUsd);
              } else {
                // Fallback to default prices based on token symbol
                if (pairData.token1Symbol === 'USDC' || pairData.token1Symbol === 'DAI') token1Price = 1;
                else if (pairData.token1Symbol === 'WETH') token1Price = 3000;
                else if (pairData.token1Symbol === 'TWI') token1Price = 50;
                else if (pairData.token1Symbol === 'WBTC') token1Price = 120000;
                else token1Price = 1;
              }
            }
          } catch (error) {
            console.error(`Error getting token prices for pair ${pairAddress}:`, error);
            // Use default prices
            if (pairData.token0Symbol === 'USDC' || pairData.token0Symbol === 'DAI') token0Price = 1;
            else if (pairData.token0Symbol === 'WETH') token0Price = 3000;
            else if (pairData.token0Symbol === 'TWI') token0Price = 50;
            else if (pairData.token0Symbol === 'WBTC') token0Price = 120000;
            else token0Price = 1;
            
            if (pairData.token1Symbol === 'USDC' || pairData.token1Symbol === 'DAI') token1Price = 1;
            else if (pairData.token1Symbol === 'WETH') token1Price = 3000;
            else if (pairData.token1Symbol === 'TWI') token1Price = 50;
            else if (pairData.token1Symbol === 'WBTC') token1Price = 120000;
            else token1Price = 1;
          }
        }
        
        // Calculate total value of reserves
        const reserve0Value = parseFloat(reserve0) * token0Price;
        const reserve1Value = parseFloat(reserve1) * token1Price;
        const totalReserveValue = reserve0Value + reserve1Value;
        
        // Calculate LP token price and user's value
        const lpTokenPrice = totalReserveValue / parseFloat(totalSupplyFormatted);
        const valueUsd = parseFloat(balanceFormatted) * lpTokenPrice;
        
        console.log(`Processing defiPosition for account ${address}: ${balanceFormatted} ${pairData.token0Symbol}-${pairData.token1Symbol} LP`);
        console.log(`  Reserves: ${reserve0} ${pairData.token0Symbol} ($${reserve0Value}) + ${reserve1} ${pairData.token1Symbol} ($${reserve1Value})`);
        console.log(`  LP Token Price: $${lpTokenPrice}, Total Value: $${valueUsd}`);
        
        // Check if asset already exists
        const existingAsset = await pgClient.query(
          `SELECT account_id FROM account_asset_view 
           WHERE account_id = $1 AND asset_type = 'defiPosition' AND biz_id = $2`,
          [accountId, pairData.id]
        );
        
        if (existingAsset.rowCount === 0) {
          // Insert if not exists
          await pgClient.query(
            `INSERT INTO account_asset_view (
              account_id, asset_type, biz_id, biz_name, 
              value, value_usd, asset_price,
              create_time, update_time
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())`,
            [
              accountId,
              'defiPosition',
              pairData.id,
              `${pairData.token0Symbol}-${pairData.token1Symbol}`,
              balanceFormatted,
              valueUsd,
              lpTokenPrice
            ]
          );
          console.log(`Inserted defiPosition balance for account ${address}: ${balanceFormatted} ${pairData.token0Symbol}-${pairData.token1Symbol} LP (${valueUsd} USD)`);
        } else {
          // Update if exists
          await pgClient.query(
            `UPDATE account_asset_view 
             SET value = $1, 
                 value_usd = $2, 
                 asset_price = $3,
                 update_time = NOW()
             WHERE account_id = $4 
             AND asset_type = 'defiPosition' 
             AND biz_id = $5`,
            [balanceFormatted, valueUsd, lpTokenPrice, accountId, pairData.id]
          );
          console.log(`Updated defiPosition balance for account ${address}: ${balanceFormatted} ${pairData.token0Symbol}-${pairData.token1Symbol} LP (${valueUsd} USD)`);
        }
        
        // Verify the update was successful
        const verifyResult = await pgClient.query(
          `SELECT update_time FROM account_asset_view 
           WHERE account_id = $1 AND asset_type = 'defiPosition' AND biz_id = $2`,
          [accountId, pairData.id]
        );
        
        if (verifyResult.rowCount > 0) {
          console.log(`Verified defiPosition update: update_time = ${verifyResult.rows[0].update_time}`);
        } else {
          console.warn(`Failed to verify defiPosition update for account_id=${accountId}, pair=${pairData.token0Symbol}-${pairData.token1Symbol}`);
        }
      } catch (error) {
        console.error(`Error updating defiPosition asset for account ${address}, pair ${pairAddress}:`, error);
      }
    }
  }
  
  console.log("defiPosition account assets update completed");
}

/**
 * Start periodic token rolling metric updater
 * Updates token_rolling_metric table every 20 seconds with price and mcap data
 * @param {Object} deployment - Deployment information
 * @param {Object} redis - Redis client
 * @param {Object} pgClient - PostgreSQL client
 */
async function startTokenRollingMetricUpdater(deployment, redis, pgClient) {
  // Cache for token contracts to avoid recreating them on each update
  const tokenContractCache = new Map();
  
  // Track consecutive failures for health monitoring
  let consecutiveFailures = 0;
  const MAX_CONSECUTIVE_FAILURES = 5;
  
  const updateTokenRollingMetrics = async () => {
    try {
      console.log('Updating token rolling metrics...');
      
      // Calculate aligned end time (nearest 20s)
      const now = new Date();
      const seconds = now.getSeconds();
      const alignedSeconds = Math.floor(seconds / 20) * 20;
      now.setSeconds(alignedSeconds, 0);
      const endTime = now.toISOString();
      
      console.log(`Aligned end time: ${endTime}`);
      
      // Get token data from database - only fetch once per update cycle
      const tokenResult = await pgClient.query('SELECT id, token_address, token_decimals, token_symbol FROM token');
      
      if (tokenResult.rows.length === 0) {
        console.warn('No tokens found in database, skipping update');
        return;
      }
      
      console.log(`Found ${tokenResult.rows.length} tokens to update`);
      
      // Batch Redis requests for all token prices
      const tokenAddresses = tokenResult.rows.map(row => row.token_address.toLowerCase());
      const redisKeys = tokenAddresses.map(addr => `token_price:${addr}`);
      
      // Get all prices in a single Redis pipeline
      let redisResults;
      try {
        const pipeline = redis.pipeline();
        redisKeys.forEach(key => pipeline.get(key));
        redisResults = await pipeline.exec();
        console.log(`Retrieved ${redisResults.length} token prices from Redis`);
      } catch (redisError) {
        console.error('Failed to get prices from Redis:', redisError);
        // Continue with empty results, will use default prices
        redisResults = tokenAddresses.map(() => [null, null]);
      }
      
      // Create a map of token address to price
      const priceMap = new Map();
      redisResults.forEach((result, index) => {
        const [error, price] = result;
        if (!error && price) {
          priceMap.set(tokenAddresses[index], price);
        }
      });
      
      // Prepare batch insert query
      const insertValues = [];
      const insertParams = [];
      let paramIndex = 1;
      
      // Process each token with retry logic for blockchain calls
      for (const tokenData of tokenResult.rows) {
        const tokenId = tokenData.id;
        const tokenAddress = tokenData.token_address.toLowerCase();
        const tokenSymbol = tokenData.token_symbol;
        
        // Get token price from Redis map with fallback
        let price = priceMap.get(tokenAddress);
        if (!price) {
          console.warn(`No price found in Redis for ${tokenSymbol}, using default price: 1`);
          price = '1';
        }
        
        // Retry blockchain calls up to 3 times
        let totalSupplyFormatted = null;
        let retryCount = 0;
        const MAX_RETRIES = 3;
        
        while (retryCount < MAX_RETRIES && totalSupplyFormatted === null) {
          try {
            // Get or create token contract instance
            let tokenContract = tokenContractCache.get(tokenAddress);
            if (!tokenContract) {
              tokenContract = new ethers.Contract(
                tokenData.token_address,
                ERC20_ABI,
                ethers.provider
              );
              tokenContractCache.set(tokenAddress, tokenContract);
            }
            
            // Get total supply from chain
            const totalSupply = await tokenContract.totalSupply();
            totalSupplyFormatted = ethers.formatUnits(totalSupply, tokenData.token_decimals);
            
            // Calculate market cap
            const mcap = parseFloat(totalSupplyFormatted) * parseFloat(price);
            
            console.log(`Rolling metrics for ${tokenSymbol}: price=${price}, mcap=${mcap}, end_time=${endTime}`);
            
            // Add to batch parameters
            insertValues.push(`($${paramIndex}, $${paramIndex + 1}, $${paramIndex + 2}, $${paramIndex + 3})`);
            insertParams.push(tokenId, endTime, price, mcap.toString());
            paramIndex += 4;
            
            break; // Success, exit retry loop
          } catch (error) {
            retryCount++;
            console.error(`Error processing token ${tokenSymbol} (attempt ${retryCount}/${MAX_RETRIES}):`, error);
            
            if (retryCount < MAX_RETRIES) {
              // Wait before retry with exponential backoff
              const backoffMs = Math.min(100 * Math.pow(2, retryCount), 1000);
              console.log(`Retrying in ${backoffMs}ms...`);
              await new Promise(resolve => setTimeout(resolve, backoffMs));
            }
          }
        }
        
        if (totalSupplyFormatted === null) {
          console.error(`Failed to get total supply for ${tokenSymbol} after ${MAX_RETRIES} attempts, skipping`);
        }
      }
      
      // Execute batch insert if we have values
      if (insertValues.length > 0) {
        try {
          const query = `
            INSERT INTO token_rolling_metric (token_id, end_time, token_price_usd, mcap)
            VALUES ${insertValues.join(', ')}
            ON CONFLICT (token_id, end_time) DO UPDATE SET
              token_price_usd = EXCLUDED.token_price_usd,
              mcap = EXCLUDED.mcap
          `;
          
          await pgClient.query(query, insertParams);
          console.log(`Updated rolling metrics for ${insertValues.length} tokens`);
          
          // Reset consecutive failures counter on success
          consecutiveFailures = 0;
        } catch (dbError) {
          console.error('Database error during batch insert:', dbError);
          consecutiveFailures++;
          
          // Try individual inserts as fallback
          if (consecutiveFailures <= MAX_CONSECUTIVE_FAILURES) {
            console.log('Attempting individual inserts as fallback...');
            let successCount = 0;
            
            for (let i = 0; i < insertValues.length; i++) {
              try {
                const singleQuery = `
                  INSERT INTO token_rolling_metric (token_id, end_time, token_price_usd, mcap)
                  VALUES ($1, $2, $3, $4)
                  ON CONFLICT (token_id, end_time) DO UPDATE SET
                    token_price_usd = $3,
                    mcap = $4
                `;
                
                const startIdx = i * 4;
                const params = insertParams.slice(startIdx, startIdx + 4);
                
                await pgClient.query(singleQuery, params);
                successCount++;
              } catch (singleError) {
                console.error(`Error on individual insert for token_id=${insertParams[i * 4]}:`, singleError);
              }
            }
            
            console.log(`Individual inserts: ${successCount}/${insertValues.length} successful`);
            
            if (successCount > 0) {
              consecutiveFailures = 0; // Reset if at least one succeeded
            }
          }
        }
      } else {
        console.warn('No token metrics to update');
        consecutiveFailures++;
      }
      
      // Check health status
      if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
        console.error(`WARNING: Token rolling metrics updater has failed ${consecutiveFailures} times consecutively`);
        // In a production system, you might want to send alerts or take recovery actions here
      }
      
      console.log('Token rolling metrics update completed');
    } catch (error) {
      console.error('Error updating token rolling metrics:', error);
      consecutiveFailures++;
    }
  };
  
  // Calculate delay to align with 20-second intervals
  const now = new Date();
  const seconds = now.getSeconds();
  const remainder = seconds % 20;
  const initialDelay = remainder === 0 ? 0 : (20 - remainder) * 1000;
  
  console.log(`Scheduling first token rolling metrics update in ${initialDelay}ms to align with 20s intervals`);
  
  // Schedule first update with delay to align with 20s intervals
  setTimeout(() => {
    // Run initial update
    updateTokenRollingMetrics();
    
    // Then schedule updates every 20 seconds
    setInterval(updateTokenRollingMetrics, 20000);
    
    console.log('Token rolling metrics updater started');
  }, initialDelay);
}

// Import price updater
const { startPriceUpdater } = require('./priceUpdater');

module.exports = {
  startTokenMetricsUpdater,
  startAccountAssetUpdater,
  startTokenRollingMetricUpdater,
  startPriceUpdater
}; 
