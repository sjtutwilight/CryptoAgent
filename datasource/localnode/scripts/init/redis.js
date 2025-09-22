/**
 * Redis Initialization Module
 * Handles initialization of Redis with token prices and metadata
 */

const { ethers } = require('hardhat');

/**
 * Initialize Redis with token prices and metadata
 * @param {Object} redis - Redis client
 * @param {Object} deployment - Deployment information
 */
async function initializeRedis(redis, deployment) {
  try {
    console.log("Starting Redis initialization...");
    
    // Clear existing token prices and metadata
    console.log("Clearing existing data from Redis...");
    const keys = await redis.keys('token_price:*');
    const metadataKeys = await redis.keys('*Metadata');
    
    if (keys.length > 0) {
      await redis.del(...keys);
      console.log(`Cleared ${keys.length} token price keys`);
    }
    
    if (metadataKeys.length > 0) {
      await redis.del(...metadataKeys);
      console.log(`Cleared ${metadataKeys.length} metadata keys`);
    }

    // Get token configs for reference
    const tokenConfigs = require('../config').TOKEN_CONFIG;
    console.log("\n=== TOKEN PRICES FROM CONFIG ===");
    for (const config of tokenConfigs) {
      console.log(`${config.symbol}: ${config.priceUSD} USD`);
    }
    console.log("===============================\n");

    // Set prices for all tokens directly from config
    for (const token of deployment.tokens) {
      const tokenConfig = tokenConfigs.find(t => t.symbol === token.symbol);
      if (tokenConfig && tokenConfig.priceUSD) {
        await redis.set(`token_price:${token.address.toLowerCase()}`, tokenConfig.priceUSD.toString());
        console.log(`Set price for ${token.symbol} from config: ${tokenConfig.priceUSD} USD (address: ${token.address})`);
      } else {
        // Default price if not found in config
        await redis.set(`token_price:${token.address.toLowerCase()}`, "1");
        console.log(`Set default price for ${token.symbol}: 1 USD (address: ${token.address})`);
      }
    }

    // Store token metadata in Redis
    console.log("\nStoring token metadata in Redis...");
    const tokenMetadata = deployment.tokens.map(token => {
      return {
        id: parseInt(token.id), // 确保id是数字
        address: token.address.toLowerCase(),
        symbol: token.symbol,
        name: token.name || token.symbol, // Use symbol as name if name is not available
        decimals: parseInt(token.decimals || 18), // 确保decimals是数字
        chainId: token.chainId || require('../config').CHAIN_CONFIG.chainId,
        chainName: token.chainName || require('../config').CHAIN_CONFIG.chainName
      };
    });
    await redis.set('tokenMetadata', JSON.stringify(tokenMetadata));
    console.log(`Stored metadata for ${tokenMetadata.length} tokens`);

    // Store account metadata in Redis
    console.log("\nStoring account metadata in Redis...");
    const { tagStringToBitmap } = require('../utils/tagBitmap');
    
    const accountMetadata = deployment.accounts.map(account => {
      const tagBitmap = tagStringToBitmap(account.tag);
      console.log(`Account ${account.address}: tag='${account.tag}' -> bitmap=${tagBitmap}`);
      
      return {
        id: parseInt(account.id), // 确保id是数字
        address: account.address.toLowerCase(),
        tag: account.tag,  // 保留字符串标签用于兼容性
        tagBitmap: tagBitmap  // 新增位图标签
      };
    });
    await redis.set('accountMetadata', JSON.stringify(accountMetadata));
    console.log(`Stored metadata for ${accountMetadata.length} accounts with bitmap tags`);

    // Create a map of token addresses to token objects for easier lookup
    const tokenMap = new Map();
    for (const token of deployment.tokens) {
      tokenMap.set(token.address.toLowerCase(), {
        id: token.id,
        address: token.address.toLowerCase(),
        symbol: token.symbol,
        decimals: token.decimals
      });
    }

    // Store pair metadata in Redis
    console.log("\nStoring pair metadata in Redis...");
    const pairMetadata = deployment.pairs.map((pair, index) => {
      const token0Address = pair.token0.toLowerCase();
      const token1Address = pair.token1.toLowerCase();
      const token0 = tokenMap.get(token0Address);
      const token1 = tokenMap.get(token1Address);
      
      if (!token0 || !token1) {
        console.warn(`Could not find token info for pair ${pair.address}`);
        return null;
      }
      
      return {
        pairId: parseInt(pair.id || (index + 1)), // 使用pairId而不是id，并确保是数字
        pairAddress: pair.address.toLowerCase(), // 使用pairAddress而不是address
        token0: {
          id: parseInt(token0.id), // 确保id是数字
          address: token0Address,
          symbol: token0.symbol,
          decimals: parseInt(token0.decimals || 18) // 确保decimals是数字
        },
        token1: {
          id: parseInt(token1.id), // 确保id是数字
          address: token1Address,
          symbol: token1.symbol,
          decimals: parseInt(token1.decimals || 18) // 确保decimals是数字
        },
        chainId: pair.chainId || require('../config').CHAIN_CONFIG.chainId,
        chainName: pair.chainName || require('../config').CHAIN_CONFIG.chainName
      };
    }).filter(pair => pair !== null); // Filter out null pairs
    
    await redis.set('pairMetadata', JSON.stringify(pairMetadata));
    console.log(`Stored metadata for ${pairMetadata.length} pairs`);

    // Print all token prices
    await printAllTokenPrices(redis, deployment);
    
    // Print metadata summary
    await printMetadataSummary(redis);

    console.log("Successfully initialized Redis with token prices and metadata");
  } catch (error) {
    console.error("Error initializing Redis:", error);
    throw error;
  }
}

/**
 * Print all token prices from Redis
 * @param {Object} redis - Redis client
 * @param {Object} deployment - Deployment information
 */
async function printAllTokenPrices(redis, deployment) {
  console.log("\n=== ALL TOKEN PRICES IN REDIS ===");
  for (const token of deployment.tokens) {
    try {
      const price = await redis.get(`token_price:${token.address.toLowerCase()}`);
      console.log(`${token.symbol}: ${price} USD (address: ${token.address})`);
    } catch (error) {
      console.error(`Error getting price for ${token.symbol}:`, error);
    }
  }
  console.log("================================\n");
}

/**
 * Print metadata summary from Redis
 * @param {Object} redis - Redis client
 */
async function printMetadataSummary(redis) {
  console.log("\n=== METADATA SUMMARY IN REDIS ===");
  
  try {
    const tokenMetadata = await redis.get('tokenMetadata');
    const parsedTokenMetadata = JSON.parse(tokenMetadata || '[]');
    console.log(`Token metadata: ${parsedTokenMetadata.length} tokens`);
    
    const accountMetadata = await redis.get('accountMetadata');
    const parsedAccountMetadata = JSON.parse(accountMetadata || '[]');
    console.log(`Account metadata: ${parsedAccountMetadata.length} accounts`);
    
    const pairMetadata = await redis.get('pairMetadata');
    const parsedPairMetadata = JSON.parse(pairMetadata || '[]');
    console.log(`Pair metadata: ${parsedPairMetadata.length} pairs`);
  } catch (error) {
    console.error("Error printing metadata summary:", error);
  }
  
  console.log("=================================\n");
}

/**
 * Get token metadata from Redis
 * @param {Object} redis - Redis client
 * @param {string} tokenAddress - Token address
 * @returns {Promise<Object|null>} Token metadata or null if not found
 */
async function getTokenMetadata(redis, tokenAddress) {
  try {
    const tokenMetadata = await redis.get('tokenMetadata');
    const parsedTokenMetadata = JSON.parse(tokenMetadata || '[]');
    return parsedTokenMetadata.find(token => token.address.toLowerCase() === tokenAddress.toLowerCase()) || null;
  } catch (error) {
    console.error(`Error getting token metadata for ${tokenAddress}:`, error);
    return null;
  }
}

/**
 * Get account metadata from Redis
 * @param {Object} redis - Redis client
 * @param {string} accountAddress - Account address
 * @returns {Promise<Object|null>} Account metadata or null if not found
 */
async function getAccountMetadata(redis, accountAddress) {
  try {
    const accountMetadata = await redis.get('accountMetadata');
    const parsedAccountMetadata = JSON.parse(accountMetadata || '[]');
    return parsedAccountMetadata.find(account => account.address.toLowerCase() === accountAddress.toLowerCase()) || null;
  } catch (error) {
    console.error(`Error getting account metadata for ${accountAddress}:`, error);
    return null;
  }
}

/**
 * Get pair metadata from Redis
 * @param {Object} redis - Redis client
 * @param {string} pairAddress - Pair address
 * @returns {Promise<Object|null>} Pair metadata or null if not found
 */
async function getPairMetadata(redis, pairAddress) {
  try {
    const pairMetadata = await redis.get('pairMetadata');
    const parsedPairMetadata = JSON.parse(pairMetadata || '[]');
    return parsedPairMetadata.find(pair => pair.address.toLowerCase() === pairAddress.toLowerCase()) || null;
  } catch (error) {
    console.error(`Error getting pair metadata for ${pairAddress}:`, error);
    return null;
  }
}

module.exports = {
  initializeRedis,
  getTokenMetadata,
  getAccountMetadata,
  getPairMetadata
}; 