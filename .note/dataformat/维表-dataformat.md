```sql

CREATE TABLE IF NOT EXISTS dim_token_basic (
    chain_id        INT     NOT NULL,
    token_address   VARCHAR(64) NOT NULL,
    symbol          VARCHAR(32),
    name            VARCHAR(128),
    decimals        INT,
    category        VARCHAR(32),
    is_stablecoin   BOOLEAN DEFAULT FALSE,
    is_bluechip     BOOLEAN DEFAULT FALSE,
    created_block   BIGINT,
    created_time    TIMESTAMPTZ,
    extra_meta_json JSONB,
    PRIMARY KEY (chain_id, token_address)
);

CREATE TABLE IF NOT EXISTS dim_dex_pool (
    chain_id        INT     NOT NULL,
    dex_name        VARCHAR(32) NOT NULL,
    dex_version     VARCHAR(8)  NOT NULL,
    pool_address    VARCHAR(64) NOT NULL,
    token0_address  VARCHAR(64),
    token1_address  VARCHAR(64),
    fee_tier_bps    INT,
    created_block   BIGINT,
    created_time    TIMESTAMPTZ,
    is_active       BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (chain_id, pool_address)
);

CREATE TABLE IF NOT EXISTS dim_account_basic (
    chain_id        INT     NOT NULL,
    account_address VARCHAR(64) NOT NULL,
    is_contract     BOOLEAN DEFAULT FALSE,
    is_router       BOOLEAN DEFAULT FALSE,
    is_dex_contract BOOLEAN DEFAULT FALSE,
    is_cex_address  BOOLEAN DEFAULT FALSE,
    first_seen_block BIGINT,
    first_seen_time  TIMESTAMPTZ,
    label            VARCHAR(128),
    PRIMARY KEY (chain_id, account_address)
);

CREATE TABLE IF NOT EXISTS dim_account_tag_latest (
    chain_id        INT     NOT NULL,
    account_address VARCHAR(64) NOT NULL,
    is_whale        BOOLEAN,
    is_smart        BOOLEAN,
    is_bot          BOOLEAN,
    is_cex_deposit  BOOLEAN,
    vip_level       SMALLINT,
    segment         VARCHAR(64),
    updated_at      TIMESTAMPTZ,
    PRIMARY KEY (chain_id, account_address)
);
