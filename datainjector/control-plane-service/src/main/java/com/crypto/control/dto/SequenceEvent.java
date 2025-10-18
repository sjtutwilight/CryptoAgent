package com.crypto.control.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SequenceEvent {

    @JsonProperty("type")
    private String type;

    @JsonProperty("chainId")
    private String chainId;

    @JsonProperty("sequenceNumber")
    private String sequenceNumber;

    @JsonProperty("sequenceHash")
    private String sequenceHash;

    @JsonProperty("sequenceTimestamp")
    private Long sequenceTimestamp;

    @JsonProperty("processTime")
    private Long processTime;

    public long sequenceNumberAsLong() {
        if (sequenceNumber == null) {
            return -1L;
        }
        try {
            if (sequenceNumber.startsWith("0x") || sequenceNumber.startsWith("0X")) {
                return Long.parseUnsignedLong(sequenceNumber.substring(2), 16);
            }
            return Long.parseLong(sequenceNumber);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid sequenceNumber: " + sequenceNumber, ex);
        }
    }
}
