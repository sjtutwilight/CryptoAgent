# Redis Optimization

This document describes the Redis optimization implemented in the Twilight DEX project.

## Overview

Redis is used as a fast in-memory data store for frequently accessed data in the Twilight DEX system. The optimization focuses on storing not only token prices but also metadata for tokens, accounts, and pairs to improve performance and reduce database queries.

## Key Features

1. **Token Prices**: Stores token prices for quick access by various services.
2. **Metadata Storage**: Stores metadata for tokens, accounts, and pairs in Redis for fast retrieval.
3. **Helper Functions**: Provides helper functions to easily retrieve metadata by address.

## Redis Keys

The following Redis keys are used:

- `token_price:{token_address}`: Stores the price of a token in USD.
- `tokenMetadata`: Stores metadata for all tokens as a JSON array.
- `accountMetadata`: Stores metadata for all accounts as a JSON array.
- `pairMetadata`: Stores metadata for all pairs as a JSON array.

## Helper Functions

The following helper functions are provided:

- `getTokenMetadata(redis, tokenAddress)`: Retrieves token metadata by address.
- `getAccountMetadata(redis, accountAddress)`: Retrieves account metadata by address.
- `getPairMetadata(redis, pairAddress)`: Retrieves pair metadata by address.

## Usage

### Initialization

Redis is initialized during system startup in the `initialize.js` script:

```javascript
// Initialize Redis with token prices and metadata
await initializeRedis(redis, deployment);
```

### Retrieving Metadata

Metadata can be retrieved using the helper functions:

```javascript
// Get token metadata
const tokenMetadata = await getTokenMetadata(redis, tokenAddress);

// Get account metadata
const accountMetadata = await getAccountMetadata(redis, accountAddress);

// Get pair metadata
const pairMetadata = await getPairMetadata(redis, pairAddress);
```

### Retrieving Token Prices

Token prices can be retrieved directly from Redis:

```javascript
const price = await redis.get(`token_price:${tokenAddress.toLowerCase()}`);
```

## Benefits

1. **Performance**: Reduces database queries by storing frequently accessed data in memory.
2. **Consistency**: Provides a single source of truth for metadata across services.
3. **Scalability**: Allows for easy scaling of the system by reducing database load.

## Testing

A test script is provided to verify the Redis optimization:

```bash
npx hardhat run scripts/test-redis.js
```

This script tests the retrieval of token prices and metadata from Redis. 