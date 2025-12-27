package com.twilight.aggregator.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class EthereumUtils {
    private static final double WEI_TO_ETH = 1e18; // 10^18
    private static final BigDecimal WEI_TO_ETH_BD = new BigDecimal("1000000000000000000"); // 10^18

    public static double convertWeiToEth(String weiAmount) {
        if (weiAmount == null) {
            return 0.0;
        }
        try {
            return Double.parseDouble(weiAmount) / WEI_TO_ETH;
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    public static double convertWeiToEth(long weiAmount) {
        return weiAmount / WEI_TO_ETH;
    }
    
    // 新增BigDecimal版本的方法
    public static BigDecimal convertWeiToEthBD(String weiAmount) {
        if (weiAmount == null || weiAmount.isEmpty()) {
            return BigDecimal.ZERO;
        }
        try {
            BigDecimal wei = new BigDecimal(weiAmount);
            return wei.divide(WEI_TO_ETH_BD, 18, RoundingMode.HALF_UP);
        } catch (NumberFormatException e) {
            return BigDecimal.ZERO;
        }
    }
}