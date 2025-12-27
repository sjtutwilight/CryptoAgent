package com.twilight.aggregator.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PairMetric implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long pairId;
    private String timeWindow;
    private long endTime;
    private double token0Reserve = 0.0;
    private double token1Reserve = 0.0;
    private double reserveUsd = 0.0;
    private double token0VolumeUsd = 0.0;
    private double token1VolumeUsd = 0.0;
    private double volumeUsd = 0.0;
    private int txcnt = 0;

    public PairMetric(Long pairId, String timeWindow, long endTimeMillis) {
        this.pairId = pairId;
        this.timeWindow = timeWindow;
        this.endTime = endTimeMillis;
    }

    public void incrementTxCount() {
        this.txcnt++;
    }

    public void setEndTime(long endTimeMillis) {
        this.endTime = endTimeMillis;
    }

    public void setToken0Reserve(double token0Reserve) {
        this.token0Reserve = token0Reserve;
    }

    public void setToken1Reserve(double token1Reserve) {
        this.token1Reserve = token1Reserve;
    }

    public void setReserveUsd(double reserveUsd) {
        this.reserveUsd = reserveUsd;
    }

    public void setToken0VolumeUsd(double token0VolumeUsd) {
        this.token0VolumeUsd = token0VolumeUsd;
    }

    public void setToken1VolumeUsd(double token1VolumeUsd) {
        this.token1VolumeUsd = token1VolumeUsd;
    }

    public void setVolumeUsd(double volumeUsd) {
        this.volumeUsd = volumeUsd;
    }
}