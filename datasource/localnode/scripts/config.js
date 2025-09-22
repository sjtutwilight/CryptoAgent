/**
 * Twilight DEX Configuration
 * This file contains unified configuration for tokens, pairs, and accounts
 * used across the Twilight DEX application.
 */

// Chain configuration
const CHAIN_CONFIG = {
  chainId: '31337',  // Hardhat local chain
  chainName: 'ethereum'
};

// Token configuration
const TOKEN_CONFIG = [
  {
    name: "USD Coin",
    symbol: "USDC",
    decimals: 18,
    baseAmount: [100, 10000],
    priceUSD: 1,
    category: "stablecoin",
    chainId: CHAIN_CONFIG.chainId,
    chainName: CHAIN_CONFIG.chainName
  },
  {
    name: "Wrapped Ether",
    symbol: "WETH",
    decimals: 18,
    baseAmount: [0.1, 1],
    priceUSD: 3000,
    category: "wrapped",
    chainId: CHAIN_CONFIG.chainId,
    chainName: CHAIN_CONFIG.chainName
  },
  {
    name: "Dai Stablecoin",
    symbol: "DAI",
    decimals: 18,
    baseAmount: [100, 10000],
    priceUSD: 1,
    category: "stablecoin",
    chainId: CHAIN_CONFIG.chainId,
    chainName: CHAIN_CONFIG.chainName
  },
  {
    name: "Twilight Token",
    symbol: "TWI",
    decimals: 18,
    baseAmount: [10, 1000],
    priceUSD: 50,
    category: "governance",
    chainId: CHAIN_CONFIG.chainId,
    chainName: CHAIN_CONFIG.chainName
  },
  {
    name: "Wrapped Bitcoin",
    symbol: "WBTC",
    decimals: 18,
    baseAmount: [0.01, 0.1],
    priceUSD: 120000,
    category: "wrapped",
    chainId: CHAIN_CONFIG.chainId,
    chainName: CHAIN_CONFIG.chainName
  }
];

// Pair configuration
const PAIR_CONFIG = [
  {
    token0: 'USDC',
    token1: 'WETH',
    amount0: '3000',    // 3000 USDC
    amount1: '1'        // 1 WETH
  },
  {
    token0: 'WETH',
    token1: 'DAI',
    amount0: '1',       // 1 WETH
    amount1: '3000'     // 3000 DAI
  },
  {
    token0: 'USDC',
    token1: 'DAI',
    amount0: '1000',    // 1000 USDC
    amount1: '1000'     // 1000 DAI
  },
  {
    token0: 'TWI',
    token1: 'WETH',
    amount0: '60',      // 60 TWI
    amount1: '1'        // 1 WETH
  },
  {
    token0: 'WBTC',
    token1: 'WETH',
    amount0: '1',       // 1 WBTC
    amount1: '40'       // 40 WETH
  },
  {
    token0: 'USDC',
    token1: 'TWI',
    amount0: '5000',    // 5000 USDC
    amount1: '100'      // 100 TWI
  },
  {
    token0: 'USDC',
    token1: 'WBTC',
    amount0: '120000',  // 120000 USDC
    amount1: '1'        // 1 WBTC
  }
];

// Account configuration
const ACCOUNT_CONFIG = [
  {
    id: 1,
    tag: 'cex'
  },
  {
    id: 2,
    tag: 'smart_money'
  },
  {
    id: 3,
    tag: 'whale'
  },
  {
    id: 4,
    tag: 'fresh_wallet'
  },
  {
    id: 5,
    tag: 'normal'
  }
];

// Simulation parameters
const SIMULATION_CONFIG = {
  SWAP_PROBABILITY: 0.6,  // 60% chance of swap vs liquidity operations
  MIN_DELAY: 1000,        // 1 second
  MAX_DELAY: 6000         // 6 seconds
};

// Helper function to get token by symbol
function getTokenBySymbol(symbol) {
  return TOKEN_CONFIG.find(token => token.symbol === symbol);
}

// Helper function to get token price
function getTokenPrice(symbol) {
  const token = getTokenBySymbol(symbol);
  return token ? token.priceUSD : 0;
}

// Export configurations
module.exports = {
  CHAIN_CONFIG,
  TOKEN_CONFIG,
  PAIR_CONFIG,
  ACCOUNT_CONFIG,
  SIMULATION_CONFIG,
  getTokenBySymbol,
  getTokenPrice
};
