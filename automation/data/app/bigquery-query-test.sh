#!/usr/bin/env bash
# BigQuery 查询测试脚本
# 用途：测试通过 service account 提交查询并拉取结果

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
source "$ROOT_DIR/automation/infra/load-infra-env.sh"
source "$ROOT_DIR/automation/infra/app-deps.sh"

SERVICE_ACCOUNT_JSON="${GOOGLE_APPLICATION_CREDENTIALS:-/Users/yangguang/Downloads/ethereal-cache-481306-e5-80cdea2b0099.json}"

echo "============================================"
echo "BigQuery 查询测试"
echo "============================================"
echo ""

# 1. 生成 Access Token
echo "[1/3] 生成 Access Token..."
ACCESS_TOKEN=$(python3 - "$SERVICE_ACCOUNT_JSON" << 'EOF'
import json, time, jwt, requests, sys

try:
    sa = json.load(open(sys.argv[1]))
    
    payload = {
        'iss': sa['client_email'],
        'sub': sa['client_email'],
        'aud': 'https://oauth2.googleapis.com/token',
        'iat': int(time.time()),
        'exp': int(time.time()) + 3600,
        'scope': 'https://www.googleapis.com/auth/bigquery'
    }
    jwt_token = jwt.encode(payload, sa['private_key'], algorithm='RS256')
    
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        'assertion': jwt_token
    }
    resp = requests.post('https://oauth2.googleapis.com/token', data=data)
    result = resp.json()
    if 'access_token' not in result:
        print(f"Error: {result}", file=sys.stderr)
        sys.exit(1)
    print(result['access_token'])
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)
EOF
)

if [ -z "$ACCESS_TOKEN" ]; then
    echo "❌ 无法生成 Access Token"
    exit 1
fi
echo "✅ Access Token 已生成"
echo ""

# 2. 提交查询
echo "[2/3] 提交 BigQuery 查询..."

QUERY_SQL="SELECT
  block_timestamp,
  block_number,
  transaction_hash,
  log_index,
  from_address,
  to_address,
  value AS value_raw,
  SAFE_CAST(value AS BIGNUMERIC) AS value_bn,
  SAFE_CAST(value AS BIGNUMERIC) / 1e18 AS value_link
FROM \`bigquery-public-data.crypto_ethereum.token_transfers\`
WHERE token_address = '0x514910771af9ca656af840dff83e8264ecf986ca'
  AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
  AND DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
ORDER BY block_timestamp ASC, transaction_hash ASC, log_index ASC
LIMIT 100"

QUERY_REQUEST=$(cat <<EOF
{
  "query": $(echo "$QUERY_SQL" | python3 -c "import json, sys; print(json.dumps(sys.stdin.read()))"),
  "useLegacySql": false,
  "maxResults": 10
}
EOF
)

QUERY_RESPONSE=$(curl -s -X POST \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d "$QUERY_REQUEST" \
  "https://bigquery.googleapis.com/bigquery/v2/projects/ethereal-cache-481306-e5/queries")

# 检查响应
JOB_ID=$(echo "$QUERY_RESPONSE" | python3 -c "import json, sys; data=json.load(sys.stdin); print(data.get('jobReference', {}).get('jobId', ''))" 2>/dev/null || echo "")

if [ -z "$JOB_ID" ]; then
    echo "❌ 查询提交失败"
    echo "$QUERY_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$QUERY_RESPONSE"
    exit 1
fi

echo "✅ 查询已提交"
echo "   Job ID: $JOB_ID"
echo ""

# 3. 拉取结果
echo "[3/3] 拉取查询结果..."

sleep 2  # 等待查询完成

RESULTS_RESPONSE=$(curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
  "https://bigquery.googleapis.com/bigquery/v2/projects/ethereal-cache-481306-e5/queries/$JOB_ID?maxResults=10")

# 确保输出目录存在
mkdir -p runtime/data/bigquery

# 显示结果摘要
echo "$RESULTS_RESPONSE" | python3 << 'EOF'
import json, sys

try:
    data = json.load(sys.stdin)
    
    print(f"✅ 查询完成")
    print(f"   总行数: {data.get('totalRows', 'N/A')}")
    print(f"   返回行数: {len(data.get('rows', []))}")
    print(f"   是否完成: {data.get('jobComplete', False)}")
    print(f"   有 pageToken: {'pageToken' in data}")
    
    if data.get('rows'):
        print(f"\n前 3 行数据:")
        for i, row in enumerate(data['rows'][:3], 1):
            values = [cell.get('v') for cell in row.get('f', [])]
            print(f"   {i}. {values[:3]}...")  # 只显示前3个字段
    
    # 保存完整响应
    with open('runtime/data/bigquery/test-result.json', 'w') as f:
        json.dump(data, f, indent=2)
    print(f"\n完整响应已保存到: runtime/data/bigquery/test-result.json")
    
except Exception as e:
    print(f"❌ 解析失败: {e}", file=sys.stderr)
    print(sys.stdin.read())
    sys.exit(1)
EOF

echo ""
echo "============================================"
echo "✅ 测试完成"
echo "============================================"
echo ""
echo "下一步："
echo "1. 查看完整响应: cat runtime/data/bigquery/test-result.json | python3 -m json.tool | less"
echo "2. 根据响应格式更新 Worker 配置"
