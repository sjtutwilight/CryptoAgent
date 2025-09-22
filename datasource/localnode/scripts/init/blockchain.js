/**
 * Blockchain Initialization Module
 * Handles deployment of contracts and initial setup
 */

const fs = require('fs');
const path = require('path');
const { ethers } = require('hardhat');
const { TOKEN_CONFIG, PAIR_CONFIG, ACCOUNT_CONFIG } = require('../config');

/**
 * Update the INIT_CODE_HASH in TWSwapLibrary.sol
 * @returns {string} The computed INIT_CODE_HASH
 */
async function updateInitCodeHash() {
  // Get TWSwapPair bytecode
  const TWSwapPair = await ethers.getContractFactory("TWSwapPair");
  const bytecode = TWSwapPair.bytecode;
  
  // Calculate init code hash
  const COMPUTED_INIT_CODE_HASH = ethers.keccak256(bytecode);
  console.log("Computed INIT_CODE_HASH:", COMPUTED_INIT_CODE_HASH);

  // Read TWSwapLibrary.sol
  const libraryPath = path.join(__dirname, "../../contracts/TWSwap/libraries/TWSwapLibrary.sol");
  let libraryContent = fs.readFileSync(libraryPath, 'utf8');

  // Replace the old hash with the new one
  const oldHashPattern = /uint256 initCodeHash = 0x[a-fA-F0-9]+;/;
  const newHashValue = `uint256 initCodeHash = ${COMPUTED_INIT_CODE_HASH};`;
  libraryContent = libraryContent.replace(oldHashPattern, newHashValue);

  // Write back to file
  fs.writeFileSync(libraryPath, libraryContent);
  console.log("Updated INIT_CODE_HASH in TWSwapLibrary.sol");

  return COMPUTED_INIT_CODE_HASH;
}

/**
 * Initialize the blockchain with contracts and tokens
 * @returns {Object} Deployment information
 */
async function blockChainInit() {
  try {
    // First update the INIT_CODE_HASH
    const initCodeHash = await updateInitCodeHash();
    console.log("Using INIT_CODE_HASH:", initCodeHash);

    console.log("\nStarting blockchain initialization...");

    // Get signers (accounts)
    const accounts = await ethers.getSigners();
    console.log(`Using ${accounts.length} accounts for testing`);

    // Deploy CloneFactory
    console.log("Deploying CloneFactory...");
    const CloneFactory = await ethers.getContractFactory("CloneFactory");
    const cloneFactory = await CloneFactory.deploy();
    await cloneFactory.waitForDeployment();
    console.log("CloneFactory deployed to:", await cloneFactory.getAddress());

    // Deploy MyERC20 implementation
    console.log("Deploying MyERC20 implementation...");
    const MyERC20 = await ethers.getContractFactory("MyERC20");
    const myERC20Implementation = await MyERC20.deploy();
    await myERC20Implementation.waitForDeployment();
    console.log("MyERC20 implementation deployed to:", await myERC20Implementation.getAddress());

    // Deploy TWSwap contracts
    console.log("Deploying TWSwap contracts...");
    const TWSwapFactory = await ethers.getContractFactory("TWSwapFactory");
    const twSwapFactory = await TWSwapFactory.deploy();
    await twSwapFactory.waitForDeployment();
    console.log("TWSwapFactory deployed to:", await twSwapFactory.getAddress());

    const TWSwapRouter = await ethers.getContractFactory("TWSwapRouter");
    const twSwapRouter = await TWSwapRouter.deploy(await twSwapFactory.getAddress());
    await twSwapRouter.waitForDeployment();
    console.log("TWSwapRouter deployed to:", await twSwapRouter.getAddress());

    // Deploy tokens using CloneFactory
    console.log("Deploying tokens via CloneFactory...");
    const tokenConfigs = TOKEN_CONFIG;

    const tokens = [];
    for (const config of tokenConfigs) {
      console.log(`\nDeploying ${config.symbol}...`);
      
      // Clone the implementation
      const tx = await cloneFactory.clone(await myERC20Implementation.getAddress());
      const receipt = await tx.wait();
      
      // Get the cloned token address from the event
      const event = receipt.logs.find(
        log => log.fragment && log.fragment.name === 'CloneCreated'
      );
      if (!event) {
        throw new Error(`Failed to get CloneCreated event for ${config.symbol}`);
      }
      const tokenAddress = event.args.instance;
      
      // Initialize the cloned token
      const token = await ethers.getContractAt("MyERC20", tokenAddress);
      await token.initialize(config.name, config.symbol);
      console.log(`${config.symbol} deployed and initialized at:`, tokenAddress);

      // Mint tokens to all accounts
      const mintAmount = ethers.parseUnits("1000000", config.decimals);
      for (const account of accounts) {
        await token.mint(account.address, mintAmount);
        console.log(`Minted ${ethers.formatUnits(mintAmount, config.decimals)} ${config.symbol} to ${account.address}`);
      }

      tokens.push(token);
    }

    // Initialize pairs array with correct token order and amounts
    const pairs = PAIR_CONFIG;

    // Map token symbols to token contracts
    const tokenMap = {};
    for (let i = 0; i < tokenConfigs.length; i++) {
      tokenMap[tokenConfigs[i].symbol] = tokens[i];
    }

    // Add liquidity for each pair
    for (const pairConfig of pairs) {
      try {
        // Get token contracts from tokenMap
        const tokenAContract = tokenMap[pairConfig.token0];
        const tokenBContract = tokenMap[pairConfig.token1];
        
        // Get token addresses and decimals
        const tokenAAddress = await tokenAContract.getAddress();
        const tokenASymbol = await tokenAContract.symbol();
        const tokenBAddress = await tokenBContract.getAddress();
        const tokenBSymbol = await tokenBContract.symbol();
        const tokenADecimals = await tokenAContract.decimals();
        const tokenBDecimals = await tokenBContract.decimals();
        
        // Convert amounts to BigNumber
        const amountADesired = ethers.parseUnits(pairConfig.amount0, tokenADecimals);
        const amountBDesired = ethers.parseUnits(pairConfig.amount1, tokenBDecimals);
        
        // Get router address
        const routerAddress = await twSwapRouter.getAddress();
        console.log(`Router address: ${routerAddress}`);
        
        // Approve router for token transfers
        console.log(`Approving tokens for router ${routerAddress}...`);
        const approveATx = await tokenAContract.approve(routerAddress, ethers.MaxUint256);
        await approveATx.wait();
        console.log(`Approved ${pairConfig.token0}`);
        
        const approveBTx = await tokenBContract.approve(routerAddress, ethers.MaxUint256);
        await approveBTx.wait();
        console.log(`Approved ${pairConfig.token1}`);
        
        // Add liquidity
        const deadline = Math.floor(Date.now() / 1000) + 3600; // 1 hour from now
        console.log('Adding liquidity...');
        console.log(`tokenA: ${tokenASymbol}, address: ${tokenAAddress}, amount: ${ethers.formatUnits(amountADesired, tokenADecimals)}`);
        console.log(`tokenB: ${tokenBSymbol}, address: ${tokenBAddress}, amount: ${ethers.formatUnits(amountBDesired, tokenBDecimals)}`);
        
        const addLiquidityTx = await twSwapRouter.addLiquidity(
          tokenAAddress,
          tokenBAddress,
          amountADesired,
          amountBDesired,
          0, // amountAMin
          0, // amountBMin
          accounts[0].address, // to
          deadline
        );
        
        console.log("Waiting for liquidity transaction to be confirmed...");
        const receipt = await addLiquidityTx.wait();
        console.log(`Liquidity added successfully: ${receipt.hash}`);
        
        // Get pair address and verify reserves
        const pairAddress = await twSwapFactory.getPair(tokenAAddress, tokenBAddress);
        const pairContract = await ethers.getContractAt("TWSwapPair", pairAddress);
        const reserves = await pairContract.getReserves();
        console.log(`Pair ${pairConfig.token0}-${pairConfig.token1} address: ${pairAddress}`);
        console.log(`Reserves after adding liquidity: reserve0 = ${ethers.formatUnits(reserves[0], tokenADecimals)}, reserve1 = ${ethers.formatUnits(reserves[1], tokenBDecimals)}`);
      } catch (error) {
        console.error(`Error adding liquidity for pair ${pairConfig.token0}-${pairConfig.token1}:`, error);
        throw error;
      }
    }

    console.log("\nBlockchain initialization complete!");

    // Save deployment info
    const deploymentInfo = {
      factory: await twSwapFactory.getAddress(),
      router: await twSwapRouter.getAddress(),
      initCodeHash: initCodeHash,
      accounts: accounts.slice(0, ACCOUNT_CONFIG.length).map((account, index) => ({
        address: account.address,
        id: ACCOUNT_CONFIG[index].id,
        tag: ACCOUNT_CONFIG[index].tag
      })),
      tokens: await Promise.all(tokens.map(async (t, index) => ({
        address: await t.getAddress(),
        symbol: await t.symbol(),
        decimals: await t.decimals(),
        id: (index + 1).toString()
      }))),
      pairs: await Promise.all(pairs.map(async pair => {
        const token0 = tokenMap[pair.token0];
        const token1 = tokenMap[pair.token1];
        const token0Address = await token0.getAddress();
        const token1Address = await token1.getAddress();
        const pairAddress = await twSwapFactory.getPair(token0Address, token1Address);
        return {
          token0: token0Address,
          token1: token1Address,
          address: pairAddress
        };
      }))
    };

    // Convert BigInt values to strings for JSON serialization
    const deploymentInfoSerializable = {
      ...deploymentInfo,
      factory: deploymentInfo.factory.toString(),
      router: deploymentInfo.router.toString(),
      initCodeHash: deploymentInfo.initCodeHash.toString(),
      accounts: deploymentInfo.accounts.map(account => ({
        address: account.address.toString(),
        id: account.id.toString(),
        tag: account.tag
      })),
      tokens: deploymentInfo.tokens.map(token => ({
        ...token,
        address: token.address.toString(),
        decimals: token.decimals.toString()
      })),
      pairs: deploymentInfo.pairs.map(pair => ({
        token0: pair.token0.toString(),
        token1: pair.token1.toString(),
        address: pair.address.toString()
      }))
    };

    // Save deployment info to file in project root
    const deploymentPath = path.join(__dirname, "../../../deployment.json");
    fs.writeFileSync(deploymentPath, JSON.stringify(deploymentInfoSerializable, null, 2));
    console.log(`\nDeployment info saved to ${deploymentPath}`);

    return deploymentInfoSerializable;
  } catch (error) {
    console.error('Error during blockchain initialization:', error);
    throw error;
  }
}

module.exports = {
  blockChainInit,
  updateInitCodeHash
}; 