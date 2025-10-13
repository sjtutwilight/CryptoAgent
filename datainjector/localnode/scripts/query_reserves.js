const ethers = require('ethers');
const fs = require('fs');
const path = require('path');

// Load deployment info
const deployment = require('../../deployment.json');

// ABI for the pair contract (only what we need)
const PAIR_ABI = [
    "function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)",
    "function token0() external view returns (address)",
    "function token1() external view returns (address)"
];

// ABI for ERC20 (only what we need)
const ERC20_ABI = [
    "function symbol() external view returns (string)",
    "function decimals() external view returns (uint8)"
];

async function queryReserves() {
    // Connect to local node
    const provider = new ethers.JsonRpcProvider('http://localhost:8545');
    
    console.log('\nPair Reserves Summary:');
    console.log('======================\n');

    // Get token info map for quick lookup
    const tokenMap = deployment.tokens.reduce((map, token) => {
        map[token.address.toLowerCase()] = {
            ...token,
            decimals: parseInt(token.decimals)
        };
        return map;
    }, {});

    // Query each pair
    for (const pair of deployment.pairs) {
        const pairContract = new ethers.Contract(pair.address, PAIR_ABI, provider);
        
        try {
            // Get reserves
            const reserves = await pairContract.getReserves();
            
            // Get token info
            const token0 = tokenMap[pair.token0.toLowerCase()];
            const token1 = tokenMap[pair.token1.toLowerCase()];
            const sortedToken0 = token0.address < token1.address ? token0 : token1;
            const sortedToken1 = token0.address < token1.address ? token1 : token0;
            // Convert reserves to strings before formatting
            const reserve0Str = reserves[0].toString();
            const reserve1Str = reserves[1].toString();

            // Format reserves with proper decimals
            const reserve0Formatted = ethers.formatUnits(reserve0Str, token0.decimals);
            const reserve1Formatted = ethers.formatUnits(reserve1Str, token1.decimals);

            // Get block timestamp
            const block = await provider.getBlock('latest');
            const lastUpdateTime = new Date(Number(reserves[2]) * 1000).toLocaleString();

            console.log(`Pair Address: ${pair.address}`);
            console.log(`Token0: ${sortedToken0.symbol} (${sortedToken0.address})`);
            console.log(`Reserve0: ${reserve0Formatted}`);
            console.log(`Token1: ${sortedToken1.symbol} (${sortedToken1.address})`);
            console.log(`Reserve1: ${reserve1Formatted}`);
            console.log(`Last Update: ${lastUpdateTime}`);
            console.log(`Current Block: ${block.number}`);
            console.log('----------------------\n');
        } catch (err) {
            console.error(`Error querying pair ${pair.address}:`, err.message);
        }
    }

    console.log(`Total pairs queried: ${deployment.pairs.length}`);
}

queryReserves().catch(console.error); 