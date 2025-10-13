const { Client } = require('pg');

async function main() {
  console.log('Querying database...');
  
  // 创建 PostgreSQL 客户端，使用与 initialize.js 相同的配置
  const client = new Client({
    host: 'localhost',
    port: 5432,
    database: 'twilight',
    user: 'twilight',
    password: 'twilight123'
  });

  try {
    // 连接到数据库
    await client.connect();
    console.log('Connected to PostgreSQL database');

    // 查询 token_metric 表
    console.log('\n=== TOKEN METRICS IN DATABASE ===');
    const tokenMetricsResult = await client.query(`
      SELECT t.token_symbol, tm.token_price, tm.supply, tm.liquidity
      FROM token_metric tm
      JOIN token t ON tm.token_id = t.id
      ORDER BY t.token_symbol
    `);
    
    tokenMetricsResult.rows.forEach(row => {
      console.log(`${row.token_symbol}: price=${row.token_price}, supply=${row.supply}, liquidity=${row.liquidity}`);
    });
    console.log('================================\n');

    // 查询 token_holder 表
    console.log('\n=== TOKEN HOLDERS IN DATABASE ===');
    const tokenHoldersResult = await client.query(`
      SELECT t.token_symbol, th.account_address, th.ownership
      FROM token_holder th
      JOIN token t ON th.token_id = t.id
      ORDER BY t.token_symbol, th.ownership DESC
      LIMIT 15
    `);
    
    let currentToken = null;
    tokenHoldersResult.rows.forEach(row => {
      if (currentToken !== row.token_symbol) {
        currentToken = row.token_symbol;
        console.log(`\nHolders for ${row.token_symbol}:`);
      }
      console.log(`  ${row.account_address}: ${row.ownership}%`);
    });
    console.log('================================\n');

    // 查询 account_asset 表
    console.log('\n=== NATIVE ASSETS IN DATABASE ===');
    const nativeAssetsResult = await client.query(`
      SELECT a.address, aa.value
      FROM account_asset aa
      JOIN account a ON aa.account_id = a.id
      WHERE aa.asset_type = 'native'
      ORDER BY a.address
    `);
    
    nativeAssetsResult.rows.forEach(row => {
      console.log(`${row.address}: ${row.value} ETH`);
    });
    console.log('================================\n');

  } catch (error) {
    console.error('Error querying database:', error);
  } finally {
    // 关闭数据库连接
    await client.end();
    console.log('Database connection closed');
  }
}

// 运行主函数
main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  }); 