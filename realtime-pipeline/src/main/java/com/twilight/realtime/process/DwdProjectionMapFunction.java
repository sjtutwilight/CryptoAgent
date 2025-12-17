package com.twilight.realtime.process;

import com.twilight.realtime.model.AccountTag;
import com.twilight.realtime.model.DwdDexSwap;
import com.twilight.realtime.model.EnrichedSwap;
import com.twilight.realtime.model.OdsDexSwap;
import com.twilight.realtime.model.TokenPrice;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Set;

/**
 * Projects enriched swap records into the dwd_dex_swap schema.
 */
public class DwdProjectionMapFunction extends RichMapFunction<EnrichedSwap, DwdDexSwap> {
    private static final MathContext MC = new MathContext(18, RoundingMode.HALF_UP);
    private static final BigDecimal WEI_IN_ETH = new BigDecimal("1000000000000000000");
    private static final Set<String> STABLE_KEYWORDS = Set.of("usdc", "usdt", "dai", "tusd", "usdd", "uusd", "usdp", "busd", "frax");

    @Override
    public DwdDexSwap map(EnrichedSwap value) {
        if (value == null || value.getSwap() == null) {
            throw new IllegalStateException("Enriched swap without swap payload");
        }
        OdsDexSwap swap = value.getSwap();
        DwdDexSwap dwd = new DwdDexSwap();
        dwd.setChainId(swap.getChainId());
        dwd.setDexName(swap.getDexName());
        dwd.setDexVersion(swap.getDexVersion());
        dwd.setTxHash(swap.getTxHash());
        dwd.setLogIndex(swap.getLogIndex());
        dwd.setBlockNumber(swap.getBlockNumber());
        dwd.setBlockTimestamp(swap.getBlockTimestamp());
        dwd.setPoolAddress(normalize(swap.getPoolAddress()));
        dwd.setRouterAddress(normalize(swap.getRouterAddress()));
        dwd.setTraderAddress(normalize(swap.getTraderAddress()));
        dwd.setSenderAddress(normalize(swap.getSenderAddress()));
        dwd.setRecipientAddress(normalize(swap.getRecipientAddress()));

        dwd.setToken0Address(normalize(swap.getToken0Address()));
        dwd.setToken1Address(normalize(swap.getToken1Address()));
        dwd.setToken0Symbol(swap.getToken0Symbol());
        dwd.setToken1Symbol(swap.getToken1Symbol());
        dwd.setToken0Decimals(safeDecimals(swap.getToken0Decimals()));
        dwd.setToken1Decimals(safeDecimals(swap.getToken1Decimals()));

        BigDecimal token0Raw = safeDecimal(swap.getAmount0Raw());
        BigDecimal token1Raw = safeDecimal(swap.getAmount1Raw());
        BigDecimal token0InRaw = amountByDirection(swap.getAmount0Direction(), token0Raw, true);
        BigDecimal token0OutRaw = amountByDirection(swap.getAmount0Direction(), token0Raw, false);
        BigDecimal token1InRaw = amountByDirection(swap.getAmount1Direction(), token1Raw, true);
        BigDecimal token1OutRaw = amountByDirection(swap.getAmount1Direction(), token1Raw, false);

        dwd.setAmountToken0InRaw(token0InRaw);
        dwd.setAmountToken0OutRaw(token0OutRaw);
        dwd.setAmountToken1InRaw(token1InRaw);
        dwd.setAmountToken1OutRaw(token1OutRaw);

        dwd.setAmountToken0In(normalizeAmount(token0InRaw, dwd.getToken0Decimals()));
        dwd.setAmountToken0Out(normalizeAmount(token0OutRaw, dwd.getToken0Decimals()));
        dwd.setAmountToken1In(normalizeAmount(token1InRaw, dwd.getToken1Decimals()));
        dwd.setAmountToken1Out(normalizeAmount(token1OutRaw, dwd.getToken1Decimals()));

        TokenPrice price0 = value.getToken0Price();
        TokenPrice price1 = value.getToken1Price();
        dwd.setPriceToken0Usd(price0 != null ? price0.getPriceUsd() : null);
        dwd.setPriceToken1Usd(price1 != null ? price1.getPriceUsd() : null);
        dwd.setToken0McapUsd(price0 != null ? price0.getMcapUsd() : null);
        dwd.setToken1McapUsd(price1 != null ? price1.getMcapUsd() : null);

        String baseToken = determineBaseToken(swap);
        String quoteToken = determineQuoteToken(swap, baseToken);
        dwd.setBaseTokenAddress(baseToken);
        dwd.setQuoteTokenAddress(quoteToken);
        dwd.setPriceBaseInQuote(derivePrice(baseToken, quoteToken, price0, price1, swap));
        dwd.setSwapValueUsd(estimateUsdValue(dwd, price0, price1));

        dwd.setGasUsed(swap.getGasUsed());
        dwd.setEffectiveGasPriceWei(swap.getEffectiveGasPriceWei());
        BigDecimal gasNative = swap.getGasCostNative();
        if (gasNative == null && swap.getGasUsed() > 0 && swap.getEffectiveGasPriceWei() > 0) {
            gasNative = BigDecimal.valueOf(swap.getGasUsed())
                    .multiply(BigDecimal.valueOf(swap.getEffectiveGasPriceWei()));
        }
        dwd.setGasCostNative(gasNative);
        dwd.setGasCostUsd(convertWeiToUsd(gasNative, value.getNativeTokenPrice()));

        AccountTag tag = value.getTraderTag();
        if (tag != null) {
            dwd.setTraderIsWhale(tag.isWhale());
            dwd.setTraderIsSmart(tag.isSmart());
            dwd.setTraderIsBot(tag.isBot());
            dwd.setTraderSegment(tag.getSegment());
        }

        dwd.setPriceSource(resolvePriceSource(price0, price1));
        dwd.setAccountTagVersion(null);
        dwd.setIngestionTime(swap.getIngestionTime() != null ? swap.getIngestionTime() : Instant.now());
        return dwd;
    }

    private static BigDecimal safeDecimal(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }

    private static int safeDecimals(Integer decimals) {
        return decimals != null && decimals > 0 ? decimals : 18;
    }

    private static BigDecimal amountByDirection(String direction, BigDecimal amount, boolean wantIn) {
        if (amount == null || amount.signum() == 0) {
            return BigDecimal.ZERO;
        }
        String normalized = direction == null ? "" : direction.toLowerCase();
        boolean matches = wantIn ? "in".equals(normalized) : "out".equals(normalized);
        if (!matches) {
            return BigDecimal.ZERO;
        }
        return amount.abs();
    }

    private static double normalizeAmount(BigDecimal raw, int decimals) {
        if (raw == null) {
            return 0d;
        }
        BigDecimal divisor = BigDecimal.TEN.pow(Math.max(decimals, 0));
        if (divisor.equals(BigDecimal.ZERO)) {
            return raw.doubleValue();
        }
        return raw.divide(divisor, MC).doubleValue();
    }

    private static String determineBaseToken(OdsDexSwap swap) {
        String token0Address = normalize(swap.getToken0Address());
        String token1Address = normalize(swap.getToken1Address());
        boolean token0Stable = isStable(swap.getToken0Symbol());
        boolean token1Stable = isStable(swap.getToken1Symbol());
        if (token0Stable && !token1Stable) {
            return token1Address;
        }
        if (token1Stable && !token0Stable) {
            return token0Address;
        }
        return token0Address != null ? token0Address : token1Address;
    }

    private static String determineQuoteToken(OdsDexSwap swap, String baseToken) {
        String token0Address = normalize(swap.getToken0Address());
        String token1Address = normalize(swap.getToken1Address());
        if (baseToken == null) {
            return token1Address;
        }
        return baseToken.equals(token0Address) ? token1Address : token0Address;
    }

    private static Double derivePrice(String baseToken,
                                      String quoteToken,
                                      TokenPrice price0,
                                      TokenPrice price1,
                                      OdsDexSwap swap) {
        if (baseToken == null || quoteToken == null) {
            return null;
        }
        String token0Address = normalize(swap.getToken0Address());
        TokenPrice basePrice = baseToken.equals(token0Address) ? price0 : price1;
        TokenPrice quotePrice = quoteToken.equals(token0Address) ? price0 : price1;
        if (basePrice == null || quotePrice == null || quotePrice.getPriceUsd() == 0) {
            return null;
        }
        return basePrice.getPriceUsd() / quotePrice.getPriceUsd();
    }

    private static Double estimateUsdValue(DwdDexSwap dwd, TokenPrice price0, TokenPrice price1) {
        double total = 0d;
        boolean hasValue = false;
        if (price0 != null) {
            double v0 = dwd.getAmountToken0In() * price0.getPriceUsd();
            if (v0 > 0) {
                total += v0;
                hasValue = true;
            }
        }
        if (price1 != null) {
            double v1 = dwd.getAmountToken1In() * price1.getPriceUsd();
            if (v1 > 0) {
                total += v1;
                hasValue = true;
            }
        }
        return hasValue ? total : null;
    }

    private static Double convertWeiToUsd(BigDecimal wei, TokenPrice nativePrice) {
        if (wei == null || nativePrice == null || nativePrice.getPriceUsd() <= 0) {
            return null;
        }
        BigDecimal nativeAmount = wei.divide(WEI_IN_ETH, MC);
        return nativeAmount.doubleValue() * nativePrice.getPriceUsd();
    }

    private static boolean isStable(String symbol) {
        if (symbol == null) {
            return false;
        }
        return STABLE_KEYWORDS.contains(symbol.toLowerCase());
    }

    private static String resolvePriceSource(TokenPrice price0, TokenPrice price1) {
        if (price0 != null && price0.getSource() != null) {
            return price0.getSource();
        }
        if (price1 != null) {
            return price1.getSource();
        }
        return null;
    }

    private static String normalize(String address) {
        return address == null ? null : address.toLowerCase();
    }
}
