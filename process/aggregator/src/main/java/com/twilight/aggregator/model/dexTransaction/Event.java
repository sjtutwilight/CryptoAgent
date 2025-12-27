package com.twilight.aggregator.model.dexTransaction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Event implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("chainId")
    private String chainId;

    @JsonProperty("eventName")
    private String eventName;

    @JsonProperty("contractAddress")
    private String contractAddress;

    @JsonProperty("logIndex")
    private Integer logIndex;

    @JsonProperty("eventData")
    private String eventData;

    @JsonProperty("blockNumber")
    private Long blockNumber;

    @JsonProperty("topics")
    private List<String> topics = new ArrayList<>();

    @JsonProperty("decodedArgs")
    private Map<String, String> decodedArgs;

    @JsonProperty("fromAddress")
    private String fromAddress;

    @JsonProperty("timestamp")
    private Long timestamp;
}