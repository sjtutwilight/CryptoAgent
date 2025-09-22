package com.twilight.backend.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

/**
 * WebSocket推送服务
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WebSocketService {

    private final SimpMessagingTemplate messagingTemplate;

    /**
     * 推送代币价格更新
     * 
     * @param tokenId 代币ID
     * @param data 价格数据
     */
    public void pushTokenPriceUpdate(Long tokenId, Object data) {
        String destination = "/topic/token/" + tokenId + "/price";
        log.debug("推送代币价格更新: {} -> {}", destination, data);
        messagingTemplate.convertAndSend(destination, data);
    }

    /**
     * 推送代币交易量更新
     * 
     * @param tokenId 代币ID
     * @param data 交易量数据
     */
    public void pushTokenVolumeUpdate(Long tokenId, Object data) {
        String destination = "/topic/token/" + tokenId + "/volume";
        log.debug("推送代币交易量更新: {} -> {}", destination, data);
        messagingTemplate.convertAndSend(destination, data);
    }

    /**
     * 推送代币分布更新
     * 
     * @param tokenId 代币ID
     * @param data 分布数据
     */
    public void pushTokenDistributionUpdate(Long tokenId, Object data) {
        String destination = "/topic/token/" + tokenId + "/distribution";
        log.debug("推送代币分布更新: {} -> {}", destination, data);
        messagingTemplate.convertAndSend(destination, data);
    }

    /**
     * 推送实时交易数据
     * 
     * @param tokenId 代币ID
     * @param data 交易数据
     */
    public void pushRealtimeTrades(Long tokenId, Object data) {
        String destination = "/topic/token/" + tokenId + "/trades";
        log.debug("推送实时交易: {} -> {}", destination, data);
        messagingTemplate.convertAndSend(destination, data);
    }

    /**
     * 推送Top PnL更新
     * 
     * @param tokenId 代币ID
     * @param data PnL数据
     */
    public void pushTopPnLUpdate(Long tokenId, Object data) {
        String destination = "/topic/token/" + tokenId + "/pnl";
        log.debug("推送Top PnL更新: {} -> {}", destination, data);
        messagingTemplate.convertAndSend(destination, data);
    }

    /**
     * 推送账户资产更新
     * 
     * @param accountId 账户ID
     * @param data 资产数据
     */
    public void pushAccountAssetUpdate(Long accountId, Object data) {
        String destination = "/topic/account/" + accountId + "/assets";
        log.debug("推送账户资产更新: {} -> {}", destination, data);
        messagingTemplate.convertAndSend(destination, data);
    }
}
