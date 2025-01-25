package com.example.lettuce;


import io.lettuce.core.support.caching.CacheAccessor;
import org.springframework.cache.Cache;

public class LettuceCacheAccessor implements CacheAccessor<String, Object> {
    private final Cache cache;

    public LettuceCacheAccessor(Cache cache) {
        this.cache = cache;
    }

    @Override
    public Object get(String key) {
        return cache.get(key);
    }

    @Override
    public void put(String key, Object value) {
        cache.put(key, value);
    }

    @Override
    public void evict(String key) {
        cache.evict(key);
    }
}
