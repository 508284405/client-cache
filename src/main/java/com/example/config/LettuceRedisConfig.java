package com.example.config;

import com.example.lettuce.LettuceCacheManager;
import com.example.lettuce.LettuceHotKeyListener;
import com.example.lettuce.RetryStatefulRedisConnection;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

@Configuration
@ConditionalOnClass(RedisClient.class)
public class LettuceRedisConfig {
    // lettuce 配置
    @Bean
    @ConditionalOnClass(LettuceConnectionFactory.class)
    public RedisConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration("localhost", 55000);
        configuration.setDatabase(6);
        return new LettuceConnectionFactory(configuration);
    }

    @Bean
    public RedisClient redisClient() {
        return (RedisClient) ((LettuceConnectionFactory) redisConnectionFactory()).getNativeClient();
    }

    @Bean
    @DependsOn("lifecycleProcessor")
    public StatefulRedisConnection<String, Object> statefulRedisConnection() {
        return new RetryStatefulRedisConnection(redisClient());
    }

    @Bean
    @ConditionalOnMissingBean
    public LettuceCacheManager cacheManager(ApplicationEventPublisher eventPublisher,StatefulRedisConnection<String,Object> connection){
        return new LettuceCacheManager(connection,eventPublisher);
    }

    @Bean
    public LettuceHotKeyListener lettuceHotKeyListener(LettuceCacheManager cacheManager){
        return new LettuceHotKeyListener(cacheManager);
    }
}
