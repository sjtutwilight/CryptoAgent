package com.twilight.aggregator.process;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.model.ProcessEvent;
import com.twilight.aggregator.model.Token;
import com.twilight.aggregator.model.Pair;
import com.twilight.aggregator.utils.EthereumUtils;

import java.util.Map;

public class EventSplitProcessor extends ProcessFunction<ProcessEvent, Pair> {
    private static final Logger log = LoggerFactory.getLogger(EventSplitProcessor.class);
    private static final long serialVersionUID = 1L;

    // Side output for Token events
    private final OutputTag<Token> tokenOutput = new OutputTag<Token>("token-events") {
    };

    @Override
    public void processElement(ProcessEvent event, Context ctx, Collector<Pair> out) throws Exception {
        Map<String, String> decodedArgs = event.getDecodedArgs();
        if (decodedArgs == null) {
            log.warn("Decoded args is null for event: {}", event.getEventName());
            return;
        }

        if ("Swap".equals(event.getEventName())) {
            // Process Token events
            processTokenEvents(event, decodedArgs, ctx);
            // Process Pair event
            processPairEvent(event, decodedArgs, out);
        } else if ("Sync".equals(event.getEventName()) ||
                "Mint".equals(event.getEventName()) ||
                "Burn".equals(event.getEventName())) {
            // Only process pair event for these types
            processPairEvent(event, decodedArgs, out);
        }
    }

    private void processTokenEvents(ProcessEvent event, Map<String, String> decodedArgs, Context ctx) {
        // Create and emit Token0 event
        String amount0In = decodedArgs.get("amount0In");
        String amount0Out = decodedArgs.get("amount0Out");
        if (isValidAmount(amount0In) || isValidAmount(amount0Out)) {
            Token token0 = new Token();
            token0.setTokenId(event.getToken0Id());
            token0.setTokenAddress(event.getToken0Address());
            token0.setAmount(EthereumUtils.convertWeiToEth(amount0In) + EthereumUtils.convertWeiToEth(amount0Out));
            token0.setBuyOrSell(EthereumUtils.convertWeiToEth(amount0Out) > 0);
            token0.setTokenPriceUsd(event.getToken0PriceUsd());
            token0.setFromAddress(event.getFromAddress());
            token0.setFromAddressTag(event.getFromAddressTag());
            token0.setTimestamp(event.getTimestamp());
            log.info("Emitting Token0 to side output - address: {}, amount: {}, price: {}, tag: {}",
                    token0.getTokenAddress(), token0.getAmount(), token0.getTokenPriceUsd(),
                    token0.getFromAddressTag(), token0.getTimestamp());
            ctx.output(tokenOutput, token0);
        }

        // Create and emit Token1 event
        String amount1In = decodedArgs.get("amount1In");
        String amount1Out = decodedArgs.get("amount1Out");
        if (isValidAmount(amount1In) || isValidAmount(amount1Out)) {
            Token token1 = new Token();
            token1.setTokenId(event.getToken1Id());
            token1.setTokenAddress(event.getToken1Address());
            token1.setAmount(EthereumUtils.convertWeiToEth(amount1In) + EthereumUtils.convertWeiToEth(amount1Out));
            token1.setBuyOrSell(EthereumUtils.convertWeiToEth(amount1Out) > 0);
            token1.setTokenPriceUsd(event.getToken1PriceUsd());
            token1.setFromAddress(event.getFromAddress());
            token1.setFromAddressTag(event.getFromAddressTag());
            token1.setTimestamp(event.getTimestamp());
            log.info("Emitting Token1 to side output - address: {}, amount: {}, price: {}, tag: {}",
                    token1.getTokenAddress(), token1.getAmount(), token1.getTokenPriceUsd(),
                    token1.getFromAddressTag());
            ctx.output(tokenOutput, token1);
        }
    }

    private void processPairEvent(ProcessEvent event, Map<String, String> decodedArgs, Collector<Pair> out) {
        Pair pair = new Pair();
        pair.setPairId(event.getPairId());
        pair.setPairAddress(event.getContractAddress());
        pair.setToken0Address(event.getToken0Address());
        pair.setToken1Address(event.getToken1Address());
        pair.setEventName(event.getEventName());
        pair.setFromAddress(event.getFromAddress());
        pair.setTimestamp(event.getTimestamp());
        pair.setToken0PriceUsd(event.getToken0PriceUsd());
        pair.setToken1PriceUsd(event.getToken1PriceUsd());

        // Set event-specific fields and convert Wei to Eth at entry point
        switch (event.getEventName()) {
            case "Swap":
                // Convert and store amounts in Eth
                pair.setAmount0In(EthereumUtils.convertWeiToEth(decodedArgs.get("amount0In")));
                pair.setAmount0Out(EthereumUtils.convertWeiToEth(decodedArgs.get("amount0Out")));
                pair.setAmount1In(EthereumUtils.convertWeiToEth(decodedArgs.get("amount1In")));
                pair.setAmount1Out(EthereumUtils.convertWeiToEth(decodedArgs.get("amount1Out")));
                break;
            case "Sync":
                // Convert and store reserves in Eth
                pair.setReserve0(EthereumUtils.convertWeiToEth(decodedArgs.get("reserve0")));
                pair.setReserve1(EthereumUtils.convertWeiToEth(decodedArgs.get("reserve1")));
                break;
            case "Mint":
            case "Burn":
                // Convert and store amounts in Eth
                pair.setAmount0(EthereumUtils.convertWeiToEth(decodedArgs.get("amount0")));
                pair.setAmount1(EthereumUtils.convertWeiToEth(decodedArgs.get("amount1")));
                break;
        }
        if (pair.getToken0PriceUsd() == 0 || pair.getToken1PriceUsd() == 0) {
            log.error("Pair event: {}", pair);
        }
        out.collect(pair);
    }

    private boolean isValidAmount(String amount) {
        if (amount == null)
            return false;
        try {
            return Double.parseDouble(amount) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public OutputTag<Token> getTokenOutput() {
        return tokenOutput;
    }
}