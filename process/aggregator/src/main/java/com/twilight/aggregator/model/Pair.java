package com.twilight.aggregator.model;

import java.io.Serializable;

import org.checkerframework.checker.units.qual.degrees;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Pair implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long pairId;
    private String pairAddress;
    private String token0Address;
    private String token1Address;
    private double token0PriceUsd;
    private double token1PriceUsd;
    private String eventName;

    // Swap event args
    private double amount0In;
    private double amount0Out;
    private double amount1In;
    private double amount1Out;

    // Sync event args
    private double reserve0;
    private double reserve1;

    // Mint/Burn event args
    private String sender;
    private String to;
    private double amount0;
    private double amount1;

    private String fromAddress;
    private Long timestamp;
}