#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json


DEFAULT_MANIFEST_PATH = "automation/data/manifest/evm_top_tokens_enhanced.yaml"
DEFAULT_SOURCE_ID = "evm_top_tokens_enhanced"


def parse_simple_yaml(path: str) -> dict:
    data = {}
    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            data[key.strip()] = value.strip()
    return data


def parse_ts(value: str):
    if not value:
        return None
    if value.endswith("Z"):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def load_manifest(manifest_path: str) -> dict:
    manifest = parse_simple_yaml(manifest_path)
    if "path" not in manifest:
        raise ValueError(f"manifest missing path: {manifest_path}")
    return manifest


def resolve_data_path(manifest_path: str, data_path: str) -> str:
    manifest_dir = os.path.dirname(os.path.abspath(manifest_path))
    root_dir = os.path.abspath(os.path.join(manifest_dir, os.pardir, os.pardir, os.pardir))
    return os.path.abspath(os.path.join(root_dir, data_path))


def connect_postgres():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "twilight"),
        user=os.getenv("POSTGRES_USER", "twilight"),
        password=os.getenv("POSTGRES_PASSWORD", "twilight123"),
    )


def ensure_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_token (
            token_id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            symbol TEXT NOT NULL,
            slug TEXT,
            category TEXT,
            cmc_rank INTEGER,
            circulating_supply NUMERIC,
            total_supply NUMERIC,
            max_supply NUMERIC,
            platform JSONB,
            contract_addresses JSONB,
            tags JSONB,
            description TEXT,
            logo TEXT,
            website JSONB,
            technical_doc JSONB,
            twitter JSONB,
            reddit JSONB,
            message_board JSONB,
            chat JSONB,
            explorer JSONB,
            source_code JSONB,
            notice TEXT,
            date_added TIMESTAMPTZ,
            date_launched TIMESTAMPTZ,
            source_id TEXT NOT NULL,
            dataset_timestamp TIMESTAMPTZ,
            metadata_fetched_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_dim_token_symbol ON dim_token (symbol);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_dim_token_rank ON dim_token (cmc_rank);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_dim_token_source ON dim_token (source_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_dim_token_tags_gin ON dim_token USING GIN (tags);")
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_dim_token_contracts_gin ON dim_token USING GIN (contract_addresses);"
    )


def upsert_tokens(cursor, tokens, dataset_ts, metadata_ts, source_id):
    sql = """
        INSERT INTO dim_token (
            token_id,
            name,
            symbol,
            slug,
            category,
            cmc_rank,
            circulating_supply,
            total_supply,
            max_supply,
            platform,
            contract_addresses,
            tags,
            description,
            logo,
            website,
            technical_doc,
            twitter,
            reddit,
            message_board,
            chat,
            explorer,
            source_code,
            notice,
            date_added,
            date_launched,
            source_id,
            dataset_timestamp,
            metadata_fetched_at,
            updated_at
        )
        VALUES (
            %(token_id)s,
            %(name)s,
            %(symbol)s,
            %(slug)s,
            %(category)s,
            %(cmc_rank)s,
            %(circulating_supply)s,
            %(total_supply)s,
            %(max_supply)s,
            %(platform)s,
            %(contract_addresses)s,
            %(tags)s,
            %(description)s,
            %(logo)s,
            %(website)s,
            %(technical_doc)s,
            %(twitter)s,
            %(reddit)s,
            %(message_board)s,
            %(chat)s,
            %(explorer)s,
            %(source_code)s,
            %(notice)s,
            %(date_added)s,
            %(date_launched)s,
            %(source_id)s,
            %(dataset_timestamp)s,
            %(metadata_fetched_at)s,
            NOW()
        )
        ON CONFLICT (token_id) DO UPDATE SET
            name = EXCLUDED.name,
            symbol = EXCLUDED.symbol,
            slug = EXCLUDED.slug,
            category = EXCLUDED.category,
            cmc_rank = EXCLUDED.cmc_rank,
            circulating_supply = EXCLUDED.circulating_supply,
            total_supply = EXCLUDED.total_supply,
            max_supply = EXCLUDED.max_supply,
            platform = EXCLUDED.platform,
            contract_addresses = EXCLUDED.contract_addresses,
            tags = EXCLUDED.tags,
            description = EXCLUDED.description,
            logo = EXCLUDED.logo,
            website = EXCLUDED.website,
            technical_doc = EXCLUDED.technical_doc,
            twitter = EXCLUDED.twitter,
            reddit = EXCLUDED.reddit,
            message_board = EXCLUDED.message_board,
            chat = EXCLUDED.chat,
            explorer = EXCLUDED.explorer,
            source_code = EXCLUDED.source_code,
            notice = EXCLUDED.notice,
            date_added = EXCLUDED.date_added,
            date_launched = EXCLUDED.date_launched,
            source_id = EXCLUDED.source_id,
            dataset_timestamp = EXCLUDED.dataset_timestamp,
            metadata_fetched_at = EXCLUDED.metadata_fetched_at,
            updated_at = NOW();
    """
    for token in tokens:
        metadata = token.get("metadata") or {}
        cursor.execute(
            sql,
            {
                "token_id": token.get("id"),
                "name": token.get("name"),
                "symbol": token.get("symbol"),
                "slug": token.get("slug"),
                "category": metadata.get("category"),
                "cmc_rank": token.get("cmc_rank"),
                "circulating_supply": token.get("circulating_supply"),
                "total_supply": token.get("total_supply"),
                "max_supply": token.get("max_supply"),
                "platform": Json(token.get("platform")),
                "contract_addresses": Json(metadata.get("contract_address", [])),
                "tags": Json(token.get("tags", [])),
                "description": metadata.get("description"),
                "logo": metadata.get("logo"),
                "website": Json(metadata.get("website", [])),
                "technical_doc": Json(metadata.get("technical_doc", [])),
                "twitter": Json(metadata.get("twitter", [])),
                "reddit": Json(metadata.get("reddit", [])),
                "message_board": Json(metadata.get("message_board", [])),
                "chat": Json(metadata.get("chat", [])),
                "explorer": Json(metadata.get("explorer", [])),
                "source_code": Json(metadata.get("source_code", [])),
                "notice": metadata.get("notice"),
                "date_added": parse_ts(metadata.get("date_added")),
                "date_launched": parse_ts(metadata.get("date_launched")),
                "source_id": source_id,
                "dataset_timestamp": dataset_ts,
                "metadata_fetched_at": metadata_ts,
            },
        )


def main():
    manifest_path = os.getenv("EVM_TOP_TOKENS_MANIFEST", DEFAULT_MANIFEST_PATH)
    manifest = load_manifest(manifest_path)
    data_path = resolve_data_path(manifest_path, manifest["path"])
    source_id = manifest.get("id") or DEFAULT_SOURCE_ID

    with open(data_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    tokens = payload.get("tokens", [])
    dataset_ts = parse_ts(payload.get("timestamp"))
    metadata_ts = parse_ts(payload.get("metadata_fetched_at"))

    conn = connect_postgres()
    try:
        with conn:
            with conn.cursor() as cursor:
                ensure_table(cursor)
                upsert_tokens(cursor, tokens, dataset_ts, metadata_ts, source_id)
        print(f"Loaded {len(tokens)} tokens into dim_token.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
