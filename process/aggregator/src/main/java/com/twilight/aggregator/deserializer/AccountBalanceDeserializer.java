package com.twilight.aggregator.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.twilight.aggregator.model.AccountBalance;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Kafka deserializer for AccountBalance messages from Go service
 */
public class AccountBalanceDeserializer implements DeserializationSchema<AccountBalance> {
    private static final Logger log = LoggerFactory.getLogger(AccountBalanceDeserializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    static {
        objectMapper.registerModule(new JavaTimeModule());
    }
    
    @Override
    public AccountBalance deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        
        try {
            // Parse JSON from Go service
            GoAccountBalance goBalance = objectMapper.readValue(message, GoAccountBalance.class);
            
            // Convert to Java AccountBalance
            AccountBalance balance = new AccountBalance();
            balance.setAccountId(goBalance.account_id);
            balance.setObservedTime(LocalDateTime.parse(goBalance.observed_time, DateTimeFormatter.ISO_DATE_TIME));
            balance.setBlockId(goBalance.block_id);
            balance.setAssetType(goBalance.asset_type);
            balance.setBizId(goBalance.biz_id);
            balance.setAmount(new BigDecimal(goBalance.amount));
            balance.setPriceUsd(new BigDecimal(goBalance.price_usd));
            balance.setValueUsd(new BigDecimal(goBalance.value_usd));
            balance.setLabelMask(goBalance.label_mask);
            balance.setAccountAddress(goBalance.account_address);
            balance.setContractAddress(goBalance.contract_address);
            balance.setBizName(goBalance.biz_name);
            
            return balance;
            
        } catch (Exception e) {
            log.error("Failed to deserialize AccountBalance message: {}", new String(message), e);
            return null;
        }
    }
    
    @Override
    public boolean isEndOfStream(AccountBalance nextElement) {
        return false;
    }
    
    @Override
    public TypeInformation<AccountBalance> getProducedType() {
        return TypeInformation.of(AccountBalance.class);
    }
    
    // Go JSON structure (snake_case)
    private static class GoAccountBalance {
        public long account_id;
        public String observed_time;
        public long block_id;
        public String asset_type;
        public long biz_id;
        public String amount;
        public String price_usd;
        public String value_usd;
        public int label_mask;
        public String account_address;
        public String contract_address;
        public String biz_name;
    }
}
