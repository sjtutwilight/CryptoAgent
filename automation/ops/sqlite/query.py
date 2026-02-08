#!/usr/bin/env python3
"""
本地数据文件元数据管理工具 - SQLite版本

功能:
1. 使用SQLite管理数据集和文件元数据
2. 提供查询、统计、清理等功能
3. 支持从file sink自动追加元数据
"""

import os
import sys
import json
import sqlite3
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

# 尝试导入tabulate用于美化表格输出
try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False

# 路径配置
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = PROJECT_ROOT / "runtime" / "data"
METADATA_DIR = DATA_ROOT / ".metadata"
DB_FILE = METADATA_DIR / "registry.db"


class MetadataDB:
    """SQLite元数据数据库管理器"""
    
    def __init__(self, db_path: Path = DB_FILE):
        self.db_path = db_path
        self.ensure_dirs()
        self.conn = None
        self.init_db()
    
    def ensure_dirs(self):
        """确保目录存在"""
        METADATA_DIR.mkdir(parents=True, exist_ok=True)
    
    def init_db(self):
        """初始化数据库表结构"""
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        
        cursor = self.conn.cursor()
        
        # 数据源表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS datasources (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT,
            description TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 数据集表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS datasets (
            id TEXT PRIMARY KEY,
            datasource_id TEXT NOT NULL,
            domain TEXT,
            category TEXT,
            description TEXT,
            
            -- 粒度信息（JSON）
            granularity_type TEXT,
            granularity_interval TEXT,
            granularity_unit TEXT,
            
            -- 覆盖范围（JSON）
            coverage TEXT,
            
            -- Schema（JSON）
            schema TEXT,
            
            -- 存储信息
            storage_base_path TEXT,
            storage_file_pattern TEXT,
            storage_compression TEXT,
            
            -- 元数据
            role_id TEXT,
            version TEXT DEFAULT '1.0',
            tags TEXT,
            custom_meta TEXT,
            
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (datasource_id) REFERENCES datasources(id)
        )
        ''')
        
        # 文件表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_id TEXT NOT NULL,
            path TEXT NOT NULL UNIQUE,
            size_bytes INTEGER,
            record_count INTEGER,
            checksum TEXT,
            time_range_start TEXT,
            time_range_end TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
        )
        ''')
        
        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_datasets_datasource ON datasets(datasource_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_datasets_category ON datasets(category)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_dataset ON files(dataset_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)')
        
        self.conn.commit()
        
        # 初始化默认数据源
        self._init_default_datasources()
    
    def _init_default_datasources(self):
        """初始化默认数据源"""
        default_sources = [
            ("binance", "Binance", "exchange", "币安交易所数据"),
            ("dune", "Dune Analytics", "api", "Dune Analytics API 数据"),
            ("bigquery", "Google BigQuery", "warehouse", "BigQuery 查询结果"),
            ("ethereum", "Ethereum", "blockchain", "以太坊链上数据"),
        ]
        
        cursor = self.conn.cursor()
        for ds_id, name, ds_type, desc in default_sources:
            cursor.execute('''
            INSERT OR IGNORE INTO datasources (id, name, type, description)
            VALUES (?, ?, ?, ?)
            ''', (ds_id, name, ds_type, desc))
        
        self.conn.commit()
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
    
    def add_or_update_dataset(self, dataset: Dict) -> bool:
        """添加或更新数据集"""
        cursor = self.conn.cursor()
        
        # 提取字段
        dataset_id = dataset.get('id')
        if not dataset_id:
            return False
        
        granularity = dataset.get('granularity', {})
        coverage = dataset.get('coverage', {})
        schema = dataset.get('schema', {})
        storage = dataset.get('storage', {})
        metadata = dataset.get('metadata', {})
        
        try:
            cursor.execute('''
            INSERT OR REPLACE INTO datasets (
                id, datasource_id, domain, category, description,
                granularity_type, granularity_interval, granularity_unit,
                coverage, schema,
                storage_base_path, storage_file_pattern, storage_compression,
                role_id, version, tags, custom_meta,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                COALESCE((SELECT created_at FROM datasets WHERE id = ?), CURRENT_TIMESTAMP),
                CURRENT_TIMESTAMP)
            ''', (
                dataset_id,
                dataset.get('datasource', 'unknown'),
                dataset.get('domain', ''),
                dataset.get('category', ''),
                dataset.get('description', ''),
                granularity.get('type', ''),
                granularity.get('interval', ''),
                granularity.get('unit', ''),
                json.dumps(coverage) if coverage else None,
                json.dumps(schema) if schema else None,
                storage.get('base_path', ''),
                storage.get('file_pattern', ''),
                storage.get('compression', 'none'),
                metadata.get('role_id', ''),
                metadata.get('version', '1.0'),
                json.dumps(metadata.get('tags', [])),
                json.dumps(metadata.get('custom_meta', {})) if metadata.get('custom_meta') else None,
                dataset_id  # for COALESCE created_at
            ))
            
            self.conn.commit()
            return True
        
        except Exception as e:
            print(f"添加数据集失败: {e}")
            self.conn.rollback()
            return False
    
    def add_file(self, dataset_id: str, file_info: Dict) -> bool:
        """添加文件记录"""
        cursor = self.conn.cursor()
        
        try:
            time_range = file_info.get('time_range', {})
            
            cursor.execute('''
            INSERT OR REPLACE INTO files (
                dataset_id, path, size_bytes, record_count, checksum,
                time_range_start, time_range_end, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 
                COALESCE((SELECT created_at FROM files WHERE path = ?), CURRENT_TIMESTAMP))
            ''', (
                dataset_id,
                file_info.get('path', ''),
                file_info.get('size_bytes', 0),
                file_info.get('record_count', 0),
                file_info.get('checksum', ''),
                time_range.get('start', ''),
                time_range.get('end', ''),
                file_info.get('path', '')  # for COALESCE
            ))
            
            self.conn.commit()
            return True
        
        except Exception as e:
            print(f"添加文件失败: {e}")
            self.conn.rollback()
            return False
    
    def get_dataset(self, dataset_id: str) -> Optional[Dict]:
        """获取数据集详情"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM datasets WHERE id = ?', (dataset_id,))
        row = cursor.fetchone()
        
        if not row:
            return None
        
        # 获取文件列表
        cursor.execute('''
        SELECT path, size_bytes, record_count, checksum, 
               time_range_start, time_range_end, created_at
        FROM files WHERE dataset_id = ? ORDER BY created_at
        ''', (dataset_id,))
        files = cursor.fetchall()
        
        # 计算统计
        cursor.execute('''
        SELECT COUNT(*) as file_count, 
               COALESCE(SUM(size_bytes), 0) as total_size,
               COALESCE(SUM(record_count), 0) as total_records
        FROM files WHERE dataset_id = ?
        ''', (dataset_id,))
        stats = cursor.fetchone()
        
        # 构建返回字典
        dataset = dict(row)
        
        # 解析JSON字段
        if dataset.get('coverage'):
            dataset['coverage'] = json.loads(dataset['coverage'])
        if dataset.get('schema'):
            dataset['schema'] = json.loads(dataset['schema'])
        if dataset.get('tags'):
            dataset['tags'] = json.loads(dataset['tags'])
        if dataset.get('custom_meta'):
            dataset['custom_meta'] = json.loads(dataset['custom_meta'])
        
        # 构建granularity
        dataset['granularity'] = {
            'type': dataset.pop('granularity_type', ''),
            'interval': dataset.pop('granularity_interval', ''),
            'unit': dataset.pop('granularity_unit', '')
        }
        
        # 添加存储信息
        dataset['storage'] = {
            'base_path': dataset.pop('storage_base_path', ''),
            'file_pattern': dataset.pop('storage_file_pattern', ''),
            'compression': dataset.pop('storage_compression', 'none'),
            'total_files': stats['file_count'],
            'total_size_bytes': stats['total_size'],
            'total_records': stats['total_records']
        }
        
        # 添加文件列表
        dataset['files'] = [
            {
                'path': f['path'],
                'size_bytes': f['size_bytes'],
                'record_count': f['record_count'],
                'checksum': f['checksum'],
                'time_range': {
                    'start': f['time_range_start'],
                    'end': f['time_range_end']
                } if f['time_range_start'] else None,
                'created_at': f['created_at']
            }
            for f in files
        ]
        
        return dataset
    
    def list_datasources(self) -> List[Dict]:
        """列出所有数据源"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM datasources ORDER BY id')
        return [dict(row) for row in cursor.fetchall()]
    
    def query_datasets(self, datasource: Optional[str] = None, 
                      category: Optional[str] = None,
                      tags: Optional[List[str]] = None) -> List[Dict]:
        """查询数据集"""
        query = 'SELECT * FROM datasets WHERE 1=1'
        params = []
        
        if datasource:
            query += ' AND datasource_id = ?'
            params.append(datasource)
        
        if category:
            query += ' AND category = ?'
            params.append(category)
        
        if tags:
            # SQLite JSON查询
            for tag in tags:
                query += ' AND tags LIKE ?'
                params.append(f'%"{tag}"%')
        
        query += ' ORDER BY created_at DESC'
        
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        
        results = []
        for row in cursor.fetchall():
            dataset = dict(row)
            
            # 获取统计信息
            cursor.execute('''
            SELECT COUNT(*) as file_count, 
                   COALESCE(SUM(size_bytes), 0) as total_size,
                   COALESCE(SUM(record_count), 0) as total_records
            FROM files WHERE dataset_id = ?
            ''', (dataset['id'],))
            stats = cursor.fetchone()
            
            dataset['stats'] = {
                'file_count': stats['file_count'],
                'total_size_bytes': stats['total_size'],
                'total_records': stats['total_records']
            }
            
            results.append(dataset)
        
        return results
    
    def get_stats(self) -> Dict:
        """获取全局统计"""
        cursor = self.conn.cursor()
        
        # 总体统计
        cursor.execute('''
        SELECT 
            COUNT(DISTINCT d.id) as dataset_count,
            COUNT(f.id) as file_count,
            COALESCE(SUM(f.size_bytes), 0) as total_size,
            COALESCE(SUM(f.record_count), 0) as total_records
        FROM datasets d
        LEFT JOIN files f ON d.id = f.dataset_id
        ''')
        total = cursor.fetchone()
        
        # 按数据源统计
        cursor.execute('''
        SELECT 
            d.datasource_id,
            COUNT(DISTINCT d.id) as dataset_count,
            COUNT(f.id) as file_count,
            COALESCE(SUM(f.size_bytes), 0) as total_size,
            COALESCE(SUM(f.record_count), 0) as total_records
        FROM datasets d
        LEFT JOIN files f ON d.id = f.dataset_id
        GROUP BY d.datasource_id
        ORDER BY d.datasource_id
        ''')
        by_source = cursor.fetchall()
        
        return {
            'total': dict(total),
            'by_source': [dict(row) for row in by_source]
        }
    
    def find_stale_datasets(self, days: int = 30) -> List[Dict]:
        """查找过期数据集"""
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        
        cursor = self.conn.cursor()
        cursor.execute('''
        SELECT d.*, 
               COUNT(f.id) as file_count,
               COALESCE(SUM(f.size_bytes), 0) as total_size
        FROM datasets d
        LEFT JOIN files f ON d.id = f.dataset_id
        WHERE d.updated_at < ?
        GROUP BY d.id
        ORDER BY d.updated_at
        ''', (cutoff,))
        
        return [dict(row) for row in cursor.fetchall()]
    
    def delete_dataset(self, dataset_id: str, delete_files: bool = False) -> bool:
        """删除数据集"""
        cursor = self.conn.cursor()
        
        try:
            if delete_files:
                # 获取文件列表并删除实际文件
                cursor.execute('SELECT path FROM files WHERE dataset_id = ?', (dataset_id,))
                for row in cursor.fetchall():
                    file_path = DATA_ROOT / row['path']
                    if file_path.exists():
                        file_path.unlink()
            
            # 删除文件记录（CASCADE会自动删除）
            cursor.execute('DELETE FROM datasets WHERE id = ?', (dataset_id,))
            
            self.conn.commit()
            return True
        
        except Exception as e:
            print(f"删除数据集失败: {e}")
            self.conn.rollback()
            return False


class MetadataManager:
    """元数据管理器"""
    
    def __init__(self):
        self.db = MetadataDB()
    
    def close(self):
        self.db.close()
    
    def ingest_from_json(self, json_data: Dict) -> bool:
        """从JSON数据导入（file sink生成的格式）"""
        if not json_data or 'id' not in json_data:
            return False
        
        # 添加数据集
        if not self.db.add_or_update_dataset(json_data):
            return False
        
        # 添加文件
        dataset_id = json_data['id']
        for file_info in json_data.get('files', []):
            self.db.add_file(dataset_id, file_info)
        
        return True
    
    def list_sources(self):
        """列出所有数据源"""
        sources = self.db.list_datasources()
        
        print("\n数据源列表:")
        
        if HAS_TABULATE and sources:
            table_data = [[s['id'], s['name'], s.get('type', ''), s.get('description', '')[:40]] for s in sources]
            print(tabulate(table_data, headers=['ID', '名称', '类型', '描述'], tablefmt='grid'))
        else:
            print("-" * 80)
            print(f"{'ID':<20} {'名称':<20} {'类型':<15} {'描述':<30}")
            print("-" * 80)
            for src in sources:
                print(f"{src['id']:<20} {src['name']:<20} {src['type']:<15} {src.get('description', ''):<30}")
        
        # 统计每个数据源的数据集数量
        stats = self.db.get_stats()
        print("\n数据集数量:")
        for stat in stats['by_source']:
            print(f"  {stat['datasource_id']}: {stat['dataset_count']} 个数据集")
    
    def query(self, datasource: Optional[str] = None, 
             category: Optional[str] = None,
             tags: Optional[List[str]] = None):
        """查询数据集"""
        datasets = self.db.query_datasets(datasource, category, tags)
        
        print(f"\n查询结果: {len(datasets)} 个数据集")
        
        if HAS_TABULATE and datasets:
            table_data = []
            for ds in datasets:
                stats = ds.get('stats', {})
                size_mb = stats.get('total_size_bytes', 0) / 1024 / 1024
                table_data.append([
                    ds['id'][:48] + ('...' if len(ds['id']) > 48 else ''),
                    ds.get('category', ''),
                    stats.get('file_count', 0),
                    f"{size_mb:.2f}",
                    f"{stats.get('total_records', 0):,}"
                ])
            
            print(tabulate(table_data, 
                headers=['ID', '分类', '文件数', '大小(MB)', '记录数'],
                tablefmt='grid',
                maxcolwidths=[50, 15, 10, 12, 15]))
        else:
            print("-" * 120)
            print(f"{'ID':<50} {'分类':<15} {'文件数':<10} {'大小(MB)':<15} {'记录数':<15}")
            print("-" * 120)
            for ds in datasets:
                stats = ds.get('stats', {})
                size_mb = stats.get('total_size_bytes', 0) / 1024 / 1024
                print(f"{ds['id']:<50} {ds.get('category', ''):<15} {stats.get('file_count', 0):<10} {size_mb:<15.2f} {stats.get('total_records', 0):<15}")
    
    def show(self, dataset_id: str):
        """显示数据集详情"""
        dataset = self.db.get_dataset(dataset_id)
        
        if not dataset:
            print(f"✗ 未找到数据集: {dataset_id}")
            return
        
        print(f"\n数据集详情: {dataset_id}")
        print("=" * 80)
        
        # 基本信息
        print(f"数据源: {dataset.get('datasource_id', '')}")
        print(f"领域: {dataset.get('domain', '')}")
        print(f"分类: {dataset.get('category', '')}")
        print(f"描述: {dataset.get('description', '')}")
        
        # 粒度
        if dataset.get('granularity'):
            gran = dataset['granularity']
            if gran.get('type'):
                print(f"\n粒度:")
                print(f"  类型: {gran.get('type', '')}")
                print(f"  间隔: {gran.get('interval', '')}")
                print(f"  单位: {gran.get('unit', '')}")
        
        # 存储信息
        storage = dataset.get('storage', {})
        print(f"\n存储信息:")
        print(f"  路径: {storage.get('base_path', '')}")
        print(f"  文件数: {storage.get('total_files', 0)}")
        print(f"  总大小: {storage.get('total_size_bytes', 0) / 1024 / 1024:.2f} MB")
        print(f"  总记录数: {storage.get('total_records', 0):,}")
        
        # 文件列表 - 使用表格格式
        files = dataset.get('files', [])
        if files:
            print(f"\n文件列表 ({len(files)} 个):")
            if HAS_TABULATE:
                table_data = []
                for f in files[:20]:  # 显示前20个
                    size_mb = f.get('size_bytes', 0) / 1024 / 1024
                    time_range = f.get('time_range', {})
                    time_str = ""
                    if time_range:
                        start = time_range.get('start', '')[:10] if time_range.get('start') else ''
                        end = time_range.get('end', '')[:10] if time_range.get('end') else ''
                        time_str = f"{start} ~ {end}" if start else ''
                    
                    table_data.append([
                        f.get('path', '')[:50] + ('...' if len(f.get('path', '')) > 50 else ''),
                        f"{size_mb:.2f} MB",
                        f"{f.get('record_count', 0):,}",
                        time_str,
                        f.get('created_at', '')[:19] if f.get('created_at') else ''
                    ])
                
                print(tabulate(table_data, 
                    headers=['路径', '大小', '记录数', '时间范围', '创建时间'],
                    tablefmt='grid',
                    maxcolwidths=[50, 12, 12, 20, 20]))
            else:
                print("-" * 100)
                print(f"{'路径':<50} {'大小(MB)':<15} {'记录数':<15}")
                print("-" * 100)
                for f in files[:10]:  # 只显示前10个
                    size_mb = f.get('size_bytes', 0) / 1024 / 1024
                    print(f"{f.get('path', ''):<50} {size_mb:<15.2f} {f.get('record_count', 0):<15}")
            
            if len(files) > 20:
                print(f"\n... 还有 {len(files) - 20} 个文件")
        
        # 元数据
        print(f"\n元数据:")
        print(f"  创建时间: {dataset.get('created_at', '')}")
        print(f"  更新时间: {dataset.get('updated_at', '')}")
        if dataset.get('tags'):
            print(f"  标签: {', '.join(dataset['tags'])}")
    
    def view_table(self, table_name: str, limit: int = 50):
        """查看表数据（格式化表格）"""
        cursor = self.db.conn.cursor()
        
        # 获取表结构
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        if not columns:
            print(f"✗ 表不存在: {table_name}")
            return
        
        col_names = [col[1] for col in columns]
        
        # 查询数据
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            print(f"表 {table_name} 为空")
            return
        
        print(f"\n表: {table_name} (显示前 {len(rows)} 条)")
        print("=" * 100)
        
        if HAS_TABULATE:
            # 转换数据为列表格式
            table_data = []
            for row in rows:
                table_data.append([str(row[col])[:50] if row[col] is not None else '' for col in col_names])
            
            print(tabulate(table_data, headers=col_names, tablefmt='grid', maxcolwidths=[30] * len(col_names)))
        else:
            # 简单格式
            print(" | ".join(col_names))
            print("-" * 100)
            for row in rows:
                print(" | ".join([str(row[col])[:30] if row[col] is not None else '' for col in col_names]))
        
        # 显示总数
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total = cursor.fetchone()[0]
        if total > limit:
            print(f"\n... 总共 {total} 条记录，仅显示前 {limit} 条")
    
    def stats(self):
        """统计数据集"""
        stats = self.db.get_stats()
        total = stats['total']
        
        print("\n数据集统计:")
        print("=" * 80)
        print(f"总数据集数: {total['dataset_count']}")
        print(f"总文件数: {total['file_count']:,}")
        print(f"总大小: {total['total_size'] / 1024 / 1024 / 1024:.2f} GB")
        print(f"总记录数: {total['total_records']:,}")
        
        print("\n按数据源统计:")
        print("-" * 80)
        print(f"{'数据源':<20} {'数据集数':<15} {'文件数':<15} {'大小(GB)':<15} {'记录数':<15}")
        print("-" * 80)
        
        for stat in stats['by_source']:
            size_gb = stat['total_size'] / 1024 / 1024 / 1024
            print(f"{stat['datasource_id']:<20} {stat['dataset_count']:<15} {stat['file_count']:<15,} {size_gb:<15.2f} {stat['total_records']:<15,}")
    
    def stale(self, days: int = 30):
        """列出过期数据集"""
        datasets = self.db.find_stale_datasets(days)
        
        print(f"\n过期数据集 (超过 {days} 天未更新): {len(datasets)} 个")
        print("-" * 120)
        print(f"{'ID':<50} {'最后更新':<25} {'文件数':<10} {'大小(MB)':<15}")
        print("-" * 120)
        
        for ds in datasets:
            size_mb = ds.get('total_size', 0) / 1024 / 1024
            print(f"{ds['id']:<50} {ds.get('updated_at', ''):<25} {ds.get('file_count', 0):<10} {size_mb:<15.2f}")
    
    def cleanup(self, dataset_id: str, delete_files: bool = False, confirm: bool = False):
        """清理数据集"""
        dataset = self.db.get_dataset(dataset_id)
        
        if not dataset:
            print(f"✗ 未找到数据集: {dataset_id}")
            return
        
        # 显示将要删除的信息
        storage = dataset.get('storage', {})
        print(f"\n即将删除数据集: {dataset_id}")
        print(f"  文件数: {storage.get('total_files', 0)}")
        print(f"  大小: {storage.get('total_size_bytes', 0) / 1024 / 1024:.2f} MB")
        print(f"  记录数: {storage.get('total_records', 0):,}")
        
        if delete_files:
            print(f"  ⚠️  将同时删除实际文件")
        
        if not confirm:
            print("\n⚠️  请添加 --confirm 参数确认删除")
            return
        
        # 执行删除
        if self.db.delete_dataset(dataset_id, delete_files):
            print(f"\n✓ 删除完成")
        else:
            print(f"\n✗ 删除失败")


def main():
    parser = argparse.ArgumentParser(description="本地数据文件元数据管理工具 (SQLite)")
    subparsers = parser.add_subparsers(dest="command", help="子命令")
    
    # list-sources 命令
    subparsers.add_parser("list-sources", help="列出所有数据源")
    
    # query 命令
    query_parser = subparsers.add_parser("query", help="查询数据集")
    query_parser.add_argument("--datasource", help="数据源")
    query_parser.add_argument("--category", help="分类")
    query_parser.add_argument("--tags", help="标签（逗号分隔）")
    
    # show 命令
    show_parser = subparsers.add_parser("show", help="显示数据集详情")
    show_parser.add_argument("--dataset-id", required=True, help="数据集ID")
    
    # stats 命令
    subparsers.add_parser("stats", help="统计数据集")
    
    # stale 命令
    stale_parser = subparsers.add_parser("stale", help="列出过期数据集")
    stale_parser.add_argument("--days", type=int, default=30, help="天数阈值（默认30天）")
    
    # cleanup 命令
    cleanup_parser = subparsers.add_parser("cleanup", help="清理数据集")
    cleanup_parser.add_argument("--dataset-id", required=True, help="数据集ID")
    cleanup_parser.add_argument("--delete-files", action="store_true", help="同时删除实际文件")
    cleanup_parser.add_argument("--confirm", action="store_true", help="确认删除")
    
    # ingest 命令（用于file sink调用）
    ingest_parser = subparsers.add_parser("ingest", help="导入元数据（内部命令）")
    ingest_parser.add_argument("--json-file", required=True, help="JSON文件路径")
    
    # view 命令（查看表数据）
    view_parser = subparsers.add_parser("view", help="查看表数据（格式化表格）")
    view_parser.add_argument("table", help="表名（datasources/datasets/files）")
    view_parser.add_argument("--limit", type=int, default=50, help="显示条数（默认50）")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    manager = MetadataManager()
    
    try:
        if args.command == "list-sources":
            manager.list_sources()
        
        elif args.command == "query":
            tags = args.tags.split(",") if args.tags else None
            manager.query(datasource=args.datasource, category=args.category, tags=tags)
        
        elif args.command == "show":
            manager.show(args.dataset_id)
        
        elif args.command == "stats":
            manager.stats()
        
        elif args.command == "stale":
            manager.stale(days=args.days)
        
        elif args.command == "cleanup":
            manager.cleanup(args.dataset_id, delete_files=args.delete_files, confirm=args.confirm)
        
        elif args.command == "ingest":
            # 内部命令，用于file sink调用
            with open(args.json_file, 'r') as f:
                data = json.load(f)
            
            if manager.ingest_from_json(data):
                print(f"✓ 导入成功: {data.get('id', 'unknown')}")
            else:
                print(f"✗ 导入失败")
                sys.exit(1)
        
        elif args.command == "view":
            manager.view_table(args.table, limit=args.limit)
    
    finally:
        manager.close()


if __name__ == "__main__":
    main()
