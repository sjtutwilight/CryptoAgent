package com.twilight.aggregator.model.dexTransaction;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("transactionHash")
    private String transactionHash;

    @JsonProperty("blockNumber")
    private Long blockNumber;

    @JsonProperty("blockHash")
    private String blockHash;

    @JsonProperty("timestamp")
    private Long timestamp;

    @JsonProperty("transactionStatus")
    private String transactionStatus;

    @JsonProperty("gasUsed")
    private Long gasUsed;

    @JsonProperty("gasPrice")
    private String gasPrice;

    @JsonProperty("nonce")
    private Long nonce;

    @JsonProperty("fromAddress")
    private String fromAddress;

    @JsonProperty("toAddress")
    private String toAddress;

    @JsonProperty("inputData")
    private String inputData;

    @JsonProperty("chainID")
    private String chainID;

}
