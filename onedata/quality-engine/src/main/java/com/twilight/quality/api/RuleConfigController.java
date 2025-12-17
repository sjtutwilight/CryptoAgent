package com.twilight.quality.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.quality.domain.entity.RuleConfig;
import com.twilight.quality.repository.RuleConfigRepository;
import com.twilight.quality.rule.RuleRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 规则配置API控制器
 */
@RestController
@RequestMapping("/api/quality/rules")
public class RuleConfigController {
    
    private final RuleConfigRepository ruleConfigRepository;
    private final RuleRegistry ruleRegistry;
    private final ObjectMapper objectMapper;
    
    public RuleConfigController(RuleConfigRepository ruleConfigRepository,
                                 RuleRegistry ruleRegistry,
                                 ObjectMapper objectMapper) {
        this.ruleConfigRepository = ruleConfigRepository;
        this.ruleRegistry = ruleRegistry;
        this.objectMapper = objectMapper;
    }
    
    /**
     * 获取所有规则配置
     */
    @GetMapping("/configs")
    public ResponseEntity<List<RuleConfig>> getAllConfigs() {
        return ResponseEntity.ok(ruleConfigRepository.findAll());
    }
    
    /**
     * 获取单个规则配置
     */
    @GetMapping("/configs/{ruleName}")
    public ResponseEntity<RuleConfig> getConfig(@PathVariable String ruleName) {
        return ruleConfigRepository.findByRuleName(ruleName)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
    
    /**
     * 创建或更新规则配置
     */
    @PostMapping("/configs")
    public ResponseEntity<RuleConfig> saveConfig(@RequestBody RuleConfig config) {
        // 检查规则是否存在
        if (!ruleRegistry.getRule(config.getRuleName()).isPresent()) {
            return ResponseEntity.badRequest().build();
        }
        
        // 查找已有配置
        Optional<RuleConfig> existing = ruleConfigRepository.findByRuleName(config.getRuleName());
        if (existing.isPresent()) {
            RuleConfig existingConfig = existing.get();
            existingConfig.setEnabled(config.getEnabled());
            existingConfig.setAlertLevel(config.getAlertLevel());
            existingConfig.setConfigJson(config.getConfigJson());
            existingConfig.setDescription(config.getDescription());
            config = existingConfig;
        }
        
        RuleConfig saved = ruleConfigRepository.save(config);
        
        // 应用配置到规则引擎
        applyConfigToRule(saved);
        
        return ResponseEntity.ok(saved);
    }
    
    /**
     * 启用/禁用规则
     */
    @PatchMapping("/configs/{ruleName}/enabled")
    public ResponseEntity<RuleConfig> toggleRule(
            @PathVariable String ruleName,
            @RequestParam boolean enabled) {
        
        Optional<RuleConfig> configOpt = ruleConfigRepository.findByRuleName(ruleName);
        
        RuleConfig config;
        if (configOpt.isPresent()) {
            config = configOpt.get();
            config.setEnabled(enabled);
        } else {
            // 创建新配置
            config = new RuleConfig();
            config.setRuleName(ruleName);
            config.setEnabled(enabled);
        }
        
        RuleConfig saved = ruleConfigRepository.save(config);
        applyConfigToRule(saved);
        
        return ResponseEntity.ok(saved);
    }
    
    /**
     * 删除规则配置（恢复默认）
     */
    @DeleteMapping("/configs/{ruleName}")
    public ResponseEntity<Void> deleteConfig(@PathVariable String ruleName) {
        Optional<RuleConfig> config = ruleConfigRepository.findByRuleName(ruleName);
        if (config.isPresent()) {
            ruleConfigRepository.delete(config.get());
        }
        return ResponseEntity.noContent().build();
    }
    
    /**
     * 应用配置到规则引擎
     */
    @SuppressWarnings("unchecked")
    private void applyConfigToRule(RuleConfig config) {
        ruleRegistry.getRule(config.getRuleName()).ifPresent(rule -> {
            // 解析JSON配置
            if (config.getConfigJson() != null && !config.getConfigJson().isEmpty()) {
                try {
                    Map<String, Object> configMap = objectMapper.readValue(
                            config.getConfigJson(), Map.class);
                    
                    // 添加enabled和alert_level
                    configMap.put("enabled", config.getEnabled());
                    if (config.getAlertLevel() != null) {
                        configMap.put("alert_level", config.getAlertLevel());
                    }
                    
                    rule.configure(configMap);
                } catch (JsonProcessingException e) {
                    // 忽略解析错误
                }
            }
        });
    }
}

