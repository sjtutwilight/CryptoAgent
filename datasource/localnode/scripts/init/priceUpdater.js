/**
 * 价格更新器 - 通过pair reserves动态计算token价格
 * 这个模块负责定期通过swap pair的储备量计算token相对于USDC的实时价格
 */

const { ethers } = require('hardhat');

// Pair ABI for reading reserves
const PAIR_ABI = [
  "function getReserves() view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)",
  "function token0() view returns (address)",
  "function token1() view returns (address)"
];

/**
 * 启动定期价格更新器
 * @param {Object} deployment - 部署信息
 * @param {Object} redis - Redis客户端
 * @param {Object} pgClient - PostgreSQL客户端
 */
async function startPriceUpdater(deployment, redis, pgClient) {
  console.log('🔄 Starting price updater...');
  
  const updatePrices = async () => {
    try {
      console.log('📊 Updating token prices from pair reserves...');
      
      // 获取所有tokens和pairs
      const tokens = deployment.tokens;
      const pairs = deployment.pairs;
      
      // 查找USDC token作为基准
      const usdcToken = tokens.find(t => t.symbol === 'USDC');
      if (!usdcToken) {
        console.error('USDC token not found, cannot calculate prices');
        return;
      }
      
      console.log(`📌 Using USDC (${usdcToken.address}) as price base`);
      
      // 创建token地址到token信息的映射
      const tokenMap = new Map();
      tokens.forEach(token => {
        tokenMap.set(token.address.toLowerCase(), token);
      });
      
      // 新的价格映射
      const newPrices = new Map();
      
      // 设置USDC价格为1.0
      newPrices.set(usdcToken.address.toLowerCase(), '1.0');
      
      // 遍历所有pairs，寻找与USDC配对的tokens
      for (const pair of pairs) {
        try {
          const token0Address = pair.token0.toLowerCase();
          const token1Address = pair.token1.toLowerCase();
          const usdcAddress = usdcToken.address.toLowerCase();
          
          // 检查是否是USDC pair
          let targetTokenAddress = null;
          let isToken0USDC = false;
          
          if (token0Address === usdcAddress) {
            targetTokenAddress = token1Address;
            isToken0USDC = true;
          } else if (token1Address === usdcAddress) {
            targetTokenAddress = token0Address;
            isToken0USDC = false;
          } else {
            // 不是USDC pair，跳过
            continue;
          }
          
          const targetToken = tokenMap.get(targetTokenAddress);
          if (!targetToken) {
            console.warn(`❌ Target token not found for address: ${targetTokenAddress}`);
            continue;
          }
          
          // 获取pair合约 - 确保使用正确的provider
          const provider = new ethers.JsonRpcProvider('http://localhost:8545');
          const pairContract = new ethers.Contract(pair.address, PAIR_ABI, provider);
          
          // 获取reserves
          const reserves = await pairContract.getReserves();
          const reserve0 = reserves[0];
          const reserve1 = reserves[1];
          
          // 确定USDC和目标token的reserves
          let usdcReserve, targetTokenReserve;
          if (isToken0USDC) {
            usdcReserve = reserve0;
            targetTokenReserve = reserve1;
          } else {
            usdcReserve = reserve1;
            targetTokenReserve = reserve0;
          }
          
          // 格式化reserves (从deployment配置中获取正确的小数位)
          const usdcDecimals = parseInt(usdcToken.decimals) || 18;
          const targetTokenDecimals = parseInt(targetToken.decimals) || 18;
          
          const usdcReserveFormatted = ethers.formatUnits(usdcReserve, usdcDecimals);
          const targetTokenReserveFormatted = ethers.formatUnits(targetTokenReserve, targetTokenDecimals);
          
          // 计算价格: price = usdcReserve / targetTokenReserve
          const usdcAmount = parseFloat(usdcReserveFormatted);
          const targetTokenAmount = parseFloat(targetTokenReserveFormatted);
          
          if (targetTokenAmount > 0 && usdcAmount > 0) {
            const price = usdcAmount / targetTokenAmount;
            newPrices.set(targetTokenAddress, price.toString());
            
            console.log(`💰 ${targetToken.symbol}: ${price.toFixed(6)} USD`);
            console.log(`   📊 Reserves: ${usdcAmount.toFixed(2)} USDC / ${targetTokenAmount.toFixed(2)} ${targetToken.symbol}`);
          } else {
            console.warn(`⚠️  Invalid reserves for ${targetToken.symbol}: USDC=${usdcAmount}, ${targetToken.symbol}=${targetTokenAmount}`);
          }
          
        } catch (error) {
          console.error(`❌ Error processing pair ${pair.address}:`, error.message);
        }
      }
      
      // 对于没有直接USDC pair的tokens，使用默认价格
      for (const token of tokens) {
        const address = token.address.toLowerCase();
        if (!newPrices.has(address)) {
          let defaultPrice = '1';
          
          // 基于token symbol设置合理的默认价格
          switch (token.symbol) {
            case 'WETH':
              defaultPrice = '3000';
              break;
            case 'TWI':
              defaultPrice = '50';
              break;
            case 'WBTC':
              defaultPrice = '120000';
              break;
            case 'DAI':
              defaultPrice = '1';
              break;
            default:
              defaultPrice = '1';
          }
          
          newPrices.set(address, defaultPrice);
          console.log(`🔧 ${token.symbol}: ${defaultPrice} USD (default price - no USDC pair)`);
        }
      }
      
      // 计算和更新Token指标到Redis
      console.log('💾 Calculating and updating token metrics in Redis...');
      const pipeline = redis.pipeline();
      
      for (const [address, price] of newPrices) {
        const token = tokenMap.get(address);
        const priceKey = `token_price:${address}`;
        
        // 设置价格
        pipeline.set(priceKey, price);
        
        if (token) {
          try {
            // 获取token合约获取总供应量
            const tokenContract = await ethers.getContractAt("MyERC20", address);
            const totalSupply = await tokenContract.totalSupply();
            const tokenDecimals = parseInt(token.decimals);
            const totalSupplyFormatted = parseFloat(ethers.formatUnits(totalSupply, tokenDecimals));
            
            // 计算市值 (mcap)
            const mcap = totalSupplyFormatted * parseFloat(price);
            
            // 计算FDV (对于现在的情况，FDV = mcap，因为没有锁定的token)
            const fdv = mcap;
            
            // 计算流动性 (liquidityUsd) - 从pairs计算
            let liquidityUsd = 0;
            
            // 查找包含此token的pairs
            for (const pair of pairs) {
              const token0Address = pair.token0.toLowerCase();
              const token1Address = pair.token1.toLowerCase();
              
              if (token0Address === address || token1Address === address) {
                try {
                  const provider = new ethers.JsonRpcProvider('http://localhost:8545');
                  const pairContract = new ethers.Contract(pair.address, PAIR_ABI, provider);
                  const reserves = await pairContract.getReserves();
                  
                  // 计算此pair中的流动性
                  let pairLiquidityUsd = 0;
                  
                  if (token0Address === address) {
                    // 目标token是token0，用token1的价值计算流动性
                    const token1Info = tokenMap.get(token1Address);
                    if (token1Info) {
                      const token1Price = newPrices.get(token1Address) || '1';
                      const token1Decimals = parseInt(token1Info.decimals);
                      const token1Reserve = parseFloat(ethers.formatUnits(reserves[1], token1Decimals));
                      pairLiquidityUsd = token1Reserve * parseFloat(token1Price) * 2; // 乘以2因为流动性包含两边
                    }
                  } else {
                    // 目标token是token1，用token0的价值计算流动性
                    const token0Info = tokenMap.get(token0Address);
                    if (token0Info) {
                      const token0Price = newPrices.get(token0Address) || '1';
                      const token0Decimals = parseInt(token0Info.decimals);
                      const token0Reserve = parseFloat(ethers.formatUnits(reserves[0], token0Decimals));
                      pairLiquidityUsd = token0Reserve * parseFloat(token0Price) * 2; // 乘以2因为流动性包含两边
                    }
                  }
                  
                  liquidityUsd += pairLiquidityUsd;
                } catch (pairError) {
                  console.warn(`❌ Error calculating liquidity for pair ${pair.address}:`, pairError.message);
                }
              }
            }
            
            // 如果没有找到流动性，使用默认估算（市值的一定比例）
            if (liquidityUsd === 0) {
              if (token.symbol === 'USDC' || token.symbol === 'DAI') {
                liquidityUsd = mcap * 0.8; // 稳定币有较高流动性
              } else if (token.symbol === 'WETH') {
                liquidityUsd = mcap * 0.7; // ETH有高流动性
              } else {
                liquidityUsd = mcap * 0.5; // 默认市值的50%
              }
            }
            
            // 将指标写入Redis
            pipeline.set(`token_mcap:${address}`, mcap.toString());
            pipeline.set(`token_fdv:${address}`, fdv.toString());
            pipeline.set(`token_liquidity:${address}`, liquidityUsd.toString());
            
            console.log(`✅ Redis update: ${token.symbol} - price=${price} USD, mcap=${mcap.toFixed(2)}, fdv=${fdv.toFixed(2)}, liquidity=${liquidityUsd.toFixed(2)}`);
            
          } catch (error) {
            console.warn(`❌ Error calculating metrics for ${token.symbol}:`, error.message);
            // 即使计算指标失败，也要设置价格
            console.log(`✅ Redis update: ${token.symbol} = ${price} USD (price only)`);
          }
        }
      }
      
      await pipeline.exec();
      console.log(`🚀 Successfully updated ${newPrices.size} token prices and metrics in Redis`);
      
      // 打印更新总结
      console.log('\n📈 TOKEN METRICS UPDATE SUMMARY:');
      for (const [address, price] of newPrices) {
        const token = tokenMap.get(address);
        if (token) {
          console.log(`   ${token.symbol}: $${parseFloat(price).toFixed(6)}`);
          // 注：mcap, fdv, liquidity已在上面计算时显示
        }
      }
      console.log('======================================\n');
      
    } catch (error) {
      console.error('❌ Error updating prices:', error);
    }
  };
  
  // 立即执行一次价格更新
  await updatePrices();
  
  // 每30秒更新一次价格
  setInterval(updatePrices, 30000);
  
  console.log('✅ Price updater started - updating every 30 seconds');
}

module.exports = {
  startPriceUpdater
};
