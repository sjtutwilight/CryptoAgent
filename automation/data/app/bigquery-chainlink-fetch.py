#!/usr/bin/env python3
"""
BigQuery Chainlink 转账数据拉取脚本
用途：通过 service account 查询并分页拉取 LINK token 转账数据
"""

import json
import time
import sys
import os
from pathlib import Path
from typing import Optional
import hashlib

try:
    import jwt
    import requests
except ImportError:
    print("需要安装依赖: pip install --user PyJWT requests", file=sys.stderr)
    sys.exit(1)

class BigQueryFetcher:
    def __init__(self, service_account_path: str, project_id: str):
        self.service_account_path = service_account_path
        self.project_id = project_id
        self.access_token = None
        self.token_expires_at = 0
        
    def _get_access_token(self) -> str:
        """生成或复用 access token"""
        if self.access_token and time.time() < self.token_expires_at - 60:
            return self.access_token
            
        with open(self.service_account_path) as f:
            sa = json.load(f)
        
        now = int(time.time())
        payload = {
            'iss': sa['client_email'],
            'sub': sa['client_email'],
            'aud': 'https://oauth2.googleapis.com/token',
            'iat': now,
            'exp': now + 3600,
            'scope': 'https://www.googleapis.com/auth/bigquery'
        }
        
        jwt_token = jwt.encode(payload, sa['private_key'], algorithm='RS256')
        
        resp = requests.post('https://oauth2.googleapis.com/token', data={
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': jwt_token
        })
        resp.raise_for_status()
        
        result = resp.json()
        self.access_token = result['access_token']
        self.token_expires_at = now + result.get('expires_in', 3600)
        
        return self.access_token
    
    def submit_query(self, sql: str, max_results: int = 10000) -> dict:
        """提交查询并返回 job 信息"""
        token = self._get_access_token()
        
        resp = requests.post(
            f'https://bigquery.googleapis.com/bigquery/v2/projects/{self.project_id}/queries',
            headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
            json={'query': sql, 'useLegacySql': False, 'maxResults': max_results}
        )
        resp.raise_for_status()
        
        return resp.json()
    
    def get_query_results(self, job_id: str, page_token: Optional[str] = None, max_results: int = 10000) -> dict:
        """获取查询结果（支持分页）"""
        token = self._get_access_token()
        
        params = {'maxResults': max_results}
        if page_token:
            params['pageToken'] = page_token
        
        resp = requests.get(
            f'https://bigquery.googleapis.com/bigquery/v2/projects/{self.project_id}/queries/{job_id}',
            headers={'Authorization': f'Bearer {token}'},
            params=params
        )
        resp.raise_for_status()
        
        return resp.json()
    
    def fetch_all(self, sql: str, output_dir: str, max_records_per_file: int = 50000):
        """查询并分页拉取所有数据，保存到文件"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        print(f"[1/3] 提交查询...")
        query_resp = self.submit_query(sql, max_results=10000)
        
        job_id = query_resp['jobReference']['jobId']
        total_rows = int(query_resp.get('totalRows', 0))
        
        print(f"✅ 查询已提交")
        print(f"   Job ID: {job_id}")
        print(f"   总行数: {total_rows}")
        print()
        
        print(f"[2/3] 拉取数据...")
        
        all_rows = []
        page_token = query_resp.get('pageToken')
        
        # 第一页数据
        if 'rows' in query_resp:
            all_rows.extend(query_resp['rows'])
            print(f"   已拉取: {len(all_rows)} / {total_rows}")
        
        # 后续分页
        page_num = 1
        while page_token:
            page_num += 1
            result = self.get_query_results(job_id, page_token=page_token)
            
            if 'rows' in result:
                all_rows.extend(result['rows'])
                print(f"   已拉取: {len(all_rows)} / {total_rows} (第 {page_num} 页)")
            
            page_token = result.get('pageToken')
            time.sleep(0.1)  # 避免过快请求
        
        print(f"✅ 数据拉取完成，共 {len(all_rows)} 行")
        print()
        
        print(f"[3/3] 保存文件...")
        
        # 保存 schema
        schema = query_resp.get('schema', {})
        schema_file = output_path / 'schema.json'
        schema_file.write_text(json.dumps(schema, indent=2))
        print(f"   Schema: {schema_file}")
        
        # 分文件保存数据
        file_index = 0
        files_written = []
        
        for i in range(0, len(all_rows), max_records_per_file):
            chunk = all_rows[i:i + max_records_per_file]
            filename = f"data_{file_index:04d}.json"
            filepath = output_path / filename
            
            filepath.write_text(json.dumps(chunk, indent=2))
            
            # 计算校验和
            checksum = hashlib.md5(filepath.read_bytes()).hexdigest()
            
            files_written.append({
                'filename': filename,
                'records': len(chunk),
                'size_bytes': filepath.stat().st_size,
                'md5': checksum
            })
            
            print(f"   {filename}: {len(chunk)} 行, {filepath.stat().st_size / 1024:.1f} KB")
            file_index += 1
        
        # 生成 manifest
        manifest = {
            'job_id': job_id,
            'total_rows': total_rows,
            'files': files_written,
            'schema': schema,
            'created_at': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        manifest_file = output_path / 'manifest.json'
        manifest_file.write_text(json.dumps(manifest, indent=2))
        print(f"   Manifest: {manifest_file}")
        
        print()
        print(f"✅ 全部完成！数据保存在: {output_dir}")
        
        return manifest


def main():
    # 配置
    SERVICE_ACCOUNT = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 
                                     '/Users/yangguang/Downloads/ethereal-cache-481306-e5-80cdea2b0099.json')
    PROJECT_ID = 'ethereal-cache-481306-e5'
    OUTPUT_DIR = 'runtime/data/bigquery/chainlink-transfers'
    
    # SQL 查询
    SQL = """
    SELECT
      block_timestamp,
      block_number,
      transaction_hash,
      log_index,
      from_address,
      to_address,
      value AS value_raw,
      SAFE_CAST(value AS BIGNUMERIC) AS value_bn,
      SAFE_CAST(value AS BIGNUMERIC) / 1e18 AS value_link
    FROM `bigquery-public-data.crypto_ethereum.token_transfers`
    WHERE token_address = '0x514910771af9ca656af840dff83e8264ecf986ca'
      AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
      AND DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    ORDER BY block_timestamp ASC, transaction_hash ASC, log_index ASC
    LIMIT 1000
    """
    
    print("=" * 60)
    print("BigQuery Chainlink 转账数据拉取")
    print("=" * 60)
    print()
    
    try:
        fetcher = BigQueryFetcher(SERVICE_ACCOUNT, PROJECT_ID)
        manifest = fetcher.fetch_all(SQL, OUTPUT_DIR, max_records_per_file=500)
        
        print()
        print("=" * 60)
        print("查看结果:")
        print(f"  ls -lh {OUTPUT_DIR}/")
        print(f"  cat {OUTPUT_DIR}/manifest.json | python3 -m json.tool")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ 错误: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()





