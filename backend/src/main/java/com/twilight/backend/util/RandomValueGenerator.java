package com.twilight.backend.util;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Random;

/**
 * 随机值生成器
 * 用于生成代币的age和security_score随机值
 */
@Component
public class RandomValueGenerator {

    @Value("${twilight.random.age.min:30}")
    private int ageMin;

    @Value("${twilight.random.age.max:3650}")
    private int ageMax;

    @Value("${twilight.random.security-score.min:1}")
    private int securityScoreMin;

    @Value("${twilight.random.security-score.max:100}")
    private int securityScoreMax;

    private final Random random = new Random();

    /**
     * 生成随机年龄（天数）
     * 
     * @return 随机年龄值
     */
    public Integer generateAge() {
        return ageMin + random.nextInt(ageMax - ageMin + 1);
    }

    /**
     * 生成随机安全评分
     * 
     * @return 随机安全评分
     */
    public Integer generateSecurityScore() {
        return securityScoreMin + random.nextInt(securityScoreMax - securityScoreMin + 1);
    }

    /**
     * 根据种子生成固定的随机年龄
     * 确保同一个token_id总是返回相同的随机值
     * 
     * @param seed 种子值
     * @return 固定的随机年龄
     */
    public Integer generateAge(long seed) {
        Random seededRandom = new Random(seed);
        return ageMin + seededRandom.nextInt(ageMax - ageMin + 1);
    }

    /**
     * 根据种子生成固定的随机安全评分
     * 确保同一个token_id总是返回相同的随机值
     * 
     * @param seed 种子值
     * @return 固定的随机安全评分
     */
    public Integer generateSecurityScore(long seed) {
        Random seededRandom = new Random(seed + 1000); // 使用不同的种子避免相同值
        return securityScoreMin + seededRandom.nextInt(securityScoreMax - securityScoreMin + 1);
    }
}
