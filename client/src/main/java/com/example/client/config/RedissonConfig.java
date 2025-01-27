package com.example.client.config;

import com.example.client.redisson.RedissonCacheManager;
import com.example.client.redisson.RedissonHotkeysListener;
import org.redisson.api.RedissonClient;
import org.springframework.cache.CacheManager;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
//@ConditionalOnClass(RedissonClient.class)
//@ConditionalOnBean(RedissonClient.class)
public class RedissonConfig {
    @Bean
    public CacheManager cacheManager(RedissonClient redissonClient, ApplicationEventPublisher eventPublisher) {
        return new RedissonCacheManager(redissonClient, eventPublisher);
    }

    @Bean
    public RedissonHotkeysListener redissonHotkeysListener(CacheManager cacheManager){
        return new RedissonHotkeysListener((RedissonCacheManager) cacheManager);
    }
}
