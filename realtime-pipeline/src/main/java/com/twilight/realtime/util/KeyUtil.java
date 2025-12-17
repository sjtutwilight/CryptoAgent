package com.twilight.realtime.util;

/**
 * Utility helpers for building deterministic keys inside the pipeline.
 */
public final class KeyUtil {
    private KeyUtil() {
    }

    public static String txKey(int chainId, String txHash) {
        return chainId + "#" + normalize(txHash);
    }

    public static String priceKey(int chainId, String tokenAddress) {
        return chainId + "#" + normalize(tokenAddress);
    }

    public static String swapKey(int chainId, String txHash, int logIndex) {
        return txKey(chainId, txHash) + "#" + logIndex;
    }

    private static String normalize(String value) {
        return value == null ? "" : value.toLowerCase();
    }
}
