const hre = require("hardhat");
const { ethers } = require("hardhat");
const fs = require("fs");
const path = require("path");
const { TOKEN_CONFIG, PAIR_CONFIG, SIMULATION_CONFIG, getTokenBySymbol } = require("./config");

// Simulation parameters from config
const SWAP_PROBABILITY = SIMULATION_CONFIG.SWAP_PROBABILITY;
const MIN_DELAY = SIMULATION_CONFIG.MIN_DELAY;
const MAX_DELAY = SIMULATION_CONFIG.MAX_DELAY;

// Helper function to sleep
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Helper function to get random number in range
function getRandomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// Helper function to get random float in range
function getRandomFloat(min, max) {
  return Math.random() * (max - min) + min;
}

// Helper function to get random amount based on token
function getRandomAmount(symbol, decimals) {
  const token = getTokenBySymbol(symbol);
  if (!token) {
    throw new Error(`Unknown token: ${symbol}`);
  }

  const [min, max] = token.baseAmount;
  const amount = getRandomFloat(min, max);
  return ethers.parseUnits(amount.toFixed(4), decimals);
}

// Helper function to calculate equivalent amount based on price ratios
function getEquivalentAmount(amount, fromSymbol, toSymbol, fromDecimals, toDecimals) {
  const fromToken = getTokenBySymbol(fromSymbol);
  const toToken = getTokenBySymbol(toSymbol);
  if (!fromToken || !toToken) {
    throw new Error(`Unknown token pair: ${fromSymbol}-${toSymbol}`);
  }

  const fromAmountInEth = ethers.formatUnits(amount, fromDecimals);
  const priceRatio = fromToken.priceUSD / toToken.priceUSD;
  const toAmount = parseFloat(fromAmountInEth) * priceRatio;
  
  // Add some randomness to the price (±10%)
  const randomFactor = getRandomFloat(0.9, 1.1);
  return ethers.parseUnits((toAmount * randomFactor).toFixed(4), toDecimals);
}

async function main() {
  // Get deployment addresses from initialize.js output
  const deploymentPath = path.join(__dirname, "../../deployment.json");
  if (!fs.existsSync(deploymentPath)) {
    throw new Error("Please run initialize.js first to deploy contracts");
  }
  const deployment = JSON.parse(fs.readFileSync(deploymentPath));

  // Get contracts
  const router = await ethers.getContractAt("TWSwapRouter", deployment.router);
  const factory = await ethers.getContractAt("TWSwapFactory", deployment.factory);
  
  // Get tokens
  const tokens = [];
  for (const tokenInfo of deployment.tokens) {
    const token = await ethers.getContractAt("MyERC20", tokenInfo.address);
    tokens.push({
      contract: token,
      symbol: tokenInfo.symbol,
      decimals: await token.decimals()
    });
  }

  // Get accounts
  const accounts = await ethers.getSigners();
  console.log(`Using ${accounts.length} accounts for trading`);

  // Main simulation loop
  while (true) {
    try {
      // Pick random account
      const account = accounts[getRandomInt(0, accounts.length - 1)];
      
      // Pick random token pair
      const token0Index = getRandomInt(0, tokens.length - 1);
      let token1Index;
      do {
        token1Index = getRandomInt(0, tokens.length - 1);
      } while (token1Index === token0Index);
      
      const token0 = tokens[token0Index];
      const token1 = tokens[token1Index];

      // Check if pair exists
      const pairAddress = await factory.getPair(
        await token0.contract.getAddress(),
        await token1.contract.getAddress()
      );
      
      if (pairAddress === ethers.ZeroAddress) {
        console.log(`Pair ${token0.symbol}-${token1.symbol} doesn't exist, skipping`);
        continue;
      }

      // Decide operation type
      if (Math.random() < SWAP_PROBABILITY) {
        // Perform swap
        const amount0 = getRandomAmount(token0.symbol, token0.decimals);
        await token0.contract.connect(account).approve(await router.getAddress(), amount0);
        
        console.log(`${account.address} swapping ${ethers.formatUnits(amount0, token0.decimals)} ${token0.symbol} for ${token1.symbol}`);
        
        await router.connect(account).swapExactTokensForTokens(
          amount0,
          0, // Accept any amount of token1
          [await token0.contract.getAddress(), await token1.contract.getAddress()],
          account.address,
          ethers.MaxUint256
        );
      } else {
        // Perform liquidity operation
        const isAdd = Math.random() < 0.5;
        
        if (isAdd) {
          // Add liquidity with balanced amounts based on price ratios
          const amount0 = getRandomAmount(token0.symbol, token0.decimals);
          const amount1 = getEquivalentAmount(
            amount0,
            token0.symbol,
            token1.symbol,
            token0.decimals,
            token1.decimals
          );
          
          await token0.contract.connect(account).approve(await router.getAddress(), amount0);
          await token1.contract.connect(account).approve(await router.getAddress(), amount1);
          
          console.log(
            `${account.address} adding liquidity: ` +
            `${ethers.formatUnits(amount0, token0.decimals)} ${token0.symbol} and ` +
            `${ethers.formatUnits(amount1, token1.decimals)} ${token1.symbol}`
          );
          
          await router.connect(account).addLiquidity(
            await token0.contract.getAddress(),
            await token1.contract.getAddress(),
            amount0,
            amount1,
            0, // Accept any amount of token0
            0, // Accept any amount of token1
            account.address,
            ethers.MaxUint256
          );
        } else {
          // Remove liquidity
          const pair = await ethers.getContractAt("TWSwapPair", pairAddress);
          const liquidity = await pair.balanceOf(account.address);
          
          if (liquidity > 0) {
            const removeAmount = liquidity * BigInt(getRandomInt(1, 100)) / 100n;
            
            await pair.connect(account).approve(await router.getAddress(), removeAmount);
            
            console.log(`${account.address} removing ${ethers.formatUnits(removeAmount, 18)} LP tokens from ${token0.symbol}-${token1.symbol}`);
            
            await router.connect(account).removeLiquidity(
              await token0.contract.getAddress(),
              await token1.contract.getAddress(),
              removeAmount,
              0, // Accept any amount of token0
              0, // Accept any amount of token1
              account.address,
              ethers.MaxUint256
            );
          }
        }
      }

      // Random delay between operations
      const delay = getRandomInt(MIN_DELAY, MAX_DELAY);
      console.log(`Waiting ${delay}ms before next operation...\n`);
      await sleep(delay);

    } catch (error) {
      console.error("Error in simulation:", error);
      // Continue simulation despite errors
      await sleep(1000);
    }
  }
}

// We recommend this pattern to be able to use async/await everywhere
// and properly handle errors.
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
}); 