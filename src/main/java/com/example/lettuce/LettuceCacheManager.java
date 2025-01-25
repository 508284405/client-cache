package com.example.lettuce;

import com.example.hotkeys.DecoratorHotkeyCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.lettuce.core.TrackingArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.support.caching.CacheFrontend;
import io.lettuce.core.support.caching.ClientSideCaching;
import org.springframework.cache.Cache;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.support.AbstractCacheManager;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;

public class LettuceCacheManager extends AbstractCacheManager {
    private final StatefulRedisConnection<String, Object> connection;
    private final ApplicationEventPublisher eventPublisher;

    public LettuceCacheManager(StatefulRedisConnection<String, Object> connection, ApplicationEventPublisher eventPublisher) {
        this.connection = connection;
        this.eventPublisher = eventPublisher;
    }

    @Override
    protected Collection<? extends Cache> loadCaches() {
        return Collections.emptyList();
    }

    @Override
    protected Cache getMissingCache(String name) {
        return new DecoratorHotkeyCache(new LettuceRedisCache(true, name, connection), eventPublisher);
    }

    public CacheFrontend<String, Object> createCacheFrontend(String namespace) {
        com.github.benmanes.caffeine.cache.Cache<Object, Object> caffeineCache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(100))
                .initialCapacity(100)
                .maximumSize(1000)
                .build();
        return ClientSideCaching.enable(new LettuceCacheAccessor(new CaffeineCache(namespace, caffeineCache)), connection, TrackingArgs.Builder.enabled());
    }
}
