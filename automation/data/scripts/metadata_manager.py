#!/usr/bin/env python3
"""
本地数据文件元数据管理工具

功能:
1. 合并 pending 目录的元数据片段到主注册表
2. 查询数据集元数据
3. 统计数据集大小
4. 清理过期数据集
"""

import os
import sys
import yaml
import json
import argparse
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from collections import defaultdict

# 路径配置
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_ROOT = PROJECT_ROOT / "runtime" / "data"
METADATA_DIR = DATA_ROOT / ".metadata"
REGISTRY_FILE = METADATA_DIR / "registry.yaml"
PENDING_DIR = METADATA_DIR / "pending"
ARCHIVE_DIR = METADATA_DIR / "archive"


class MetadataManager:
    """元数据管理器"""
    
    def __init__(self):
        self.ensure_dirs()
        self.registry = self.load_registry()
    
    def ensure_dirs(self):
        """确保目录存在"""
        METADATA_DIR.mkdir(parents=True, exist_ok=True)
        PENDING_DIR.mkdir(exist_ok=True)
        ARCHIVE_DIR.mkdir(exist_ok=True)
    
    def load_registry(self) -> Dict:
        """加载主注册表"""
        if not REGISTRY_FILE.exists():
            return {
                "version": "1.0",
                "last_updated": datetime.utcnow().isoformat() + "Z",
                "datasources": {},
                "datasets": []
            }
        
        with open(REGISTRY_FILE, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def save_registry(self):
        """保存主注册表"""
        # 备份旧版本
        if REGISTRY_FILE.exists():
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            archive_path = ARCHIVE_DIR / f"registry-{timestamp}.yaml"
            REGISTRY_FILE.rename(archive_path)
            print(f"✓ 备份旧版本到: {archive_path.relative_to(PROJECT_ROOT)}")
        
        # 更新时间戳
        self.registry["last_updated"] = datetime.utcnow().isoformat() + "Z"
        
        # 保存新版本
        with open(REGISTRY_FILE, 'w', encoding='utf-8') as f:
            yaml.dump(self.registry, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
        
        print(f"✓ 保存主注册表: {REGISTRY_FILE.relative_to(PROJECT_ROOT)}")
    
    def merge_pending(self) -> int:
        """合并 pending 目录的元数据片段"""
        pending_files = list(PENDING_DIR.glob("*.yaml")) + list(PENDING_DIR.glob("*.yml"))
        
        if not pending_files:
            print("✓ 没有待合并的元数据片段")
            return 0
        
        print(f"发现 {len(pending_files)} 个待合并的元数据片段")
        
        merged_count = 0
        for pending_file in pending_files:
            try:
                with open(pending_file, 'r', encoding='utf-8') as f:
                    fragment = yaml.safe_load(f)
                
                if self.merge_fragment(fragment):
                    merged_count += 1
                    # 移动到 archive
                    archive_path = ARCHIVE_DIR / pending_file.name
                    pending_file.rename(archive_path)
                    print(f"  ✓ 合并并归档: {pending_file.name}")
                else:
                    print(f"  ✗ 跳过无效片段: {pending_file.name}")
            
            except Exception as e:
                print(f"  ✗ 合并失败 {pending_file.name}: {e}")
        
        if merged_count > 0:
            self.save_registry()
        
        return merged_count
    
    def merge_fragment(self, fragment: Dict) -> bool:
        """合并单个元数据片段"""
        if not fragment or "id" not in fragment:
            return False
        
        dataset_id = fragment["id"]
        
        # 查找现有数据集
        existing_idx = None
        for idx, ds in enumerate(self.registry["datasets"]):
            if ds.get("id") == dataset_id:
                existing_idx = idx
                break
        
        if existing_idx is not None:
            # 更新现有数据集
            self.update_dataset(self.registry["datasets"][existing_idx], fragment)
        else:
            # 新增数据集
            self.registry["datasets"].append(self.normalize_dataset(fragment))
        
        return True
    
    def update_dataset(self, existing: Dict, fragment: Dict):
        """更新现有数据集"""
        # 合并文件列表
        existing_files = {f["path"]: f for f in existing.get("files", [])}
        for new_file in fragment.get("files", []):
            path = new_file["path"]
            if path in existing_files:
                # 更新文件信息
                existing_files[path].update(new_file)
            else:
                # 新增文件
                existing_files[path] = new_file
        
        existing["files"] = list(existing_files.values())
        
        # 重新计算存储统计
        if "storage" not in existing:
            existing["storage"] = {}
        
        existing["storage"]["total_files"] = len(existing["files"])
        existing["storage"]["total_size_bytes"] = sum(f.get("size_bytes", 0) for f in existing["files"])
        existing["storage"]["total_records"] = sum(f.get("record_count", 0) for f in existing["files"])
        
        # 更新时间范围
        if "coverage" in existing and "time_range" in existing["coverage"]:
            file_times = []
            for f in existing["files"]:
                if "time_range" in f:
                    if "start" in f["time_range"]:
                        file_times.append(f["time_range"]["start"])
                    if "end" in f["time_range"]:
                        file_times.append(f["time_range"]["end"])
            
            if file_times:
                file_times.sort()
                existing["coverage"]["time_range"]["start"] = file_times[0]
                existing["coverage"]["time_range"]["end"] = file_times[-1]
        
        # 更新元信息
        if "metadata" not in existing:
            existing["metadata"] = {}
        existing["metadata"]["updated_at"] = datetime.utcnow().isoformat() + "Z"
        
        # 合并其他字段
        for key in ["description", "granularity", "schema"]:
            if key in fragment:
                existing[key] = fragment[key]
    
    def normalize_dataset(self, fragment: Dict) -> Dict:
        """规范化数据集结构"""
        dataset = {
            "id": fragment["id"],
            "datasource": fragment.get("datasource", "unknown"),
            "domain": fragment.get("domain", ""),
            "category": fragment.get("category", ""),
            "description": fragment.get("description", ""),
            "granularity": fragment.get("granularity", {}),
            "coverage": fragment.get("coverage", {}),
            "schema": fragment.get("schema", {}),
            "storage": fragment.get("storage", {}),
            "files": fragment.get("files", []),
            "metadata": fragment.get("metadata", {})
        }
        
        # 计算存储统计
        if not dataset["storage"]:
            dataset["storage"] = {}
        
        dataset["storage"]["total_files"] = len(dataset["files"])
        dataset["storage"]["total_size_bytes"] = sum(f.get("size_bytes", 0) for f in dataset["files"])
        dataset["storage"]["total_records"] = sum(f.get("record_count", 0) for f in dataset["files"])
        
        # 设置创建时间
        if "created_at" not in dataset["metadata"]:
            dataset["metadata"]["created_at"] = datetime.utcnow().isoformat() + "Z"
        
        return dataset
    
    def list_sources(self):
        """列出所有数据源"""
        datasources = self.registry.get("datasources", {})
        
        print("\n数据源列表:")
        print("-" * 80)
        print(f"{'ID':<20} {'名称':<20} {'类型':<15} {'描述':<30}")
        print("-" * 80)
        
        for ds_id, ds_info in datasources.items():
            print(f"{ds_id:<20} {ds_info.get('name', ''):<20} {ds_info.get('type', ''):<15} {ds_info.get('description', ''):<30}")
        
        # 统计每个数据源的数据集数量
        source_counts = defaultdict(int)
        for ds in self.registry.get("datasets", []):
            source_counts[ds.get("datasource", "unknown")] += 1
        
        print("\n数据集数量:")
        for ds_id in datasources:
            count = source_counts.get(ds_id, 0)
            print(f"  {ds_id}: {count} 个数据集")
    
    def query(self, datasource: Optional[str] = None, tags: Optional[List[str]] = None):
        """查询数据集"""
        datasets = self.registry.get("datasets", [])
        
        # 过滤
        if datasource:
            datasets = [ds for ds in datasets if ds.get("datasource") == datasource]
        
        if tags:
            datasets = [ds for ds in datasets if any(tag in ds.get("metadata", {}).get("tags", []) for tag in tags)]
        
        print(f"\n查询结果: {len(datasets)} 个数据集")
        print("-" * 120)
        print(f"{'ID':<50} {'分类':<15} {'文件数':<10} {'大小(MB)':<15} {'记录数':<15}")
        print("-" * 120)
        
        for ds in datasets:
            storage = ds.get("storage", {})
            size_mb = storage.get("total_size_bytes", 0) / 1024 / 1024
            print(f"{ds['id']:<50} {ds.get('category', ''):<15} {storage.get('total_files', 0):<10} {size_mb:<15.2f} {storage.get('total_records', 0):<15}")
    
    def show(self, dataset_id: str):
        """显示数据集详情"""
        for ds in self.registry.get("datasets", []):
            if ds.get("id") == dataset_id:
                print(f"\n数据集详情: {dataset_id}")
                print("=" * 80)
                print(yaml.dump(ds, allow_unicode=True, sort_keys=False))
                return
        
        print(f"✗ 未找到数据集: {dataset_id}")
    
    def stats(self):
        """统计数据集"""
        datasets = self.registry.get("datasets", [])
        
        total_files = 0
        total_size = 0
        total_records = 0
        
        source_stats = defaultdict(lambda: {"files": 0, "size": 0, "records": 0})
        
        for ds in datasets:
            storage = ds.get("storage", {})
            files = storage.get("total_files", 0)
            size = storage.get("total_size_bytes", 0)
            records = storage.get("total_records", 0)
            
            total_files += files
            total_size += size
            total_records += records
            
            datasource = ds.get("datasource", "unknown")
            source_stats[datasource]["files"] += files
            source_stats[datasource]["size"] += size
            source_stats[datasource]["records"] += records
        
        print("\n数据集统计:")
        print("=" * 80)
        print(f"总数据集数: {len(datasets)}")
        print(f"总文件数: {total_files:,}")
        print(f"总大小: {total_size / 1024 / 1024 / 1024:.2f} GB")
        print(f"总记录数: {total_records:,}")
        
        print("\n按数据源统计:")
        print("-" * 80)
        print(f"{'数据源':<20} {'文件数':<15} {'大小(GB)':<15} {'记录数':<15}")
        print("-" * 80)
        
        for source, stats in sorted(source_stats.items()):
            size_gb = stats["size"] / 1024 / 1024 / 1024
            print(f"{source:<20} {stats['files']:<15,} {size_gb:<15.2f} {stats['records']:<15,}")
    
    def stale(self, days: int = 30):
        """列出过期数据集"""
        cutoff = datetime.utcnow() - timedelta(days=days)
        stale_datasets = []
        
        for ds in self.registry.get("datasets", []):
            updated_at_str = ds.get("metadata", {}).get("updated_at", "")
            if updated_at_str:
                try:
                    updated_at = datetime.fromisoformat(updated_at_str.replace("Z", "+00:00"))
                    if updated_at.replace(tzinfo=None) < cutoff:
                        stale_datasets.append(ds)
                except:
                    pass
        
        print(f"\n过期数据集 (超过 {days} 天未更新): {len(stale_datasets)} 个")
        print("-" * 120)
        print(f"{'ID':<50} {'最后更新':<25} {'文件数':<10} {'大小(MB)':<15}")
        print("-" * 120)
        
        for ds in stale_datasets:
            storage = ds.get("storage", {})
            size_mb = storage.get("total_size_bytes", 0) / 1024 / 1024
            updated_at = ds.get("metadata", {}).get("updated_at", "N/A")
            print(f"{ds['id']:<50} {updated_at:<25} {storage.get('total_files', 0):<10} {size_mb:<15.2f}")
    
    def cleanup(self, dataset_id: str, confirm: bool = False):
        """清理数据集（删除文件和元数据）"""
        dataset = None
        dataset_idx = None
        
        for idx, ds in enumerate(self.registry.get("datasets", [])):
            if ds.get("id") == dataset_id:
                dataset = ds
                dataset_idx = idx
                break
        
        if not dataset:
            print(f"✗ 未找到数据集: {dataset_id}")
            return
        
        # 显示将要删除的信息
        storage = dataset.get("storage", {})
        print(f"\n即将删除数据集: {dataset_id}")
        print(f"  文件数: {storage.get('total_files', 0)}")
        print(f"  大小: {storage.get('total_size_bytes', 0) / 1024 / 1024:.2f} MB")
        print(f"  记录数: {storage.get('total_records', 0):,}")
        
        if not confirm:
            print("\n⚠️  请添加 --confirm 参数确认删除")
            return
        
        # 删除文件
        deleted_files = 0
        for file_info in dataset.get("files", []):
            file_path = DATA_ROOT / file_info["path"]
            if file_path.exists():
                file_path.unlink()
                deleted_files += 1
        
        # 删除元数据
        self.registry["datasets"].pop(dataset_idx)
        self.save_registry()
        
        print(f"\n✓ 删除完成:")
        print(f"  删除文件: {deleted_files}")
        print(f"  删除元数据: {dataset_id}")


def main():
    parser = argparse.ArgumentParser(description="本地数据文件元数据管理工具")
    subparsers = parser.add_subparsers(dest="command", help="子命令")
    
    # merge 命令
    subparsers.add_parser("merge", help="合并 pending 目录的元数据片段")
    
    # list-sources 命令
    subparsers.add_parser("list-sources", help="列出所有数据源")
    
    # query 命令
    query_parser = subparsers.add_parser("query", help="查询数据集")
    query_parser.add_argument("--datasource", help="数据源")
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
    cleanup_parser.add_argument("--confirm", action="store_true", help="确认删除")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    manager = MetadataManager()
    
    if args.command == "merge":
        count = manager.merge_pending()
        print(f"\n✓ 合并完成，处理了 {count} 个元数据片段")
    
    elif args.command == "list-sources":
        manager.list_sources()
    
    elif args.command == "query":
        tags = args.tags.split(",") if args.tags else None
        manager.query(datasource=args.datasource, tags=tags)
    
    elif args.command == "show":
        manager.show(args.dataset_id)
    
    elif args.command == "stats":
        manager.stats()
    
    elif args.command == "stale":
        manager.stale(days=args.days)
    
    elif args.command == "cleanup":
        manager.cleanup(args.dataset_id, confirm=args.confirm)


if __name__ == "__main__":
    main()

