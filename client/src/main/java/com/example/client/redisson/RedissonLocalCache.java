package com.example.client.redisson;

import org.redisson.api.RLocalCachedMap;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.spring.cache.NullValue;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import java.lang.reflect.Constructor;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 从org.redisson.spring.cache.RedissonCache扩展
 */
public class RedissonLocalCache implements Cache {

    private RLocalCachedMap<Object, Object> mapCache;

    private final boolean allowNullValues;

    private final AtomicLong hits = new AtomicLong();

    private final AtomicLong puts = new AtomicLong();

    private final AtomicLong misses = new AtomicLong();

    public RedissonLocalCache(RLocalCachedMap<Object, Object> mapCache, boolean allowNullValues) {
        this.mapCache = mapCache;
        this.allowNullValues = allowNullValues;
    }

    @Override
    public String getName() {
        return mapCache.getName();
    }

    @Override
    public RMap<?, ?> getNativeCache() {
        return mapCache;
    }

    @Override
    public ValueWrapper get(Object key) {
        Object value = mapCache.get(key);

        if (value == null) {
            addCacheMiss();
        } else {
            addCacheHit();
        }
        return toValueWrapper(value);
    }

    public <T> T get(Object key, Class<T> type) {
        Object value = mapCache.get(key);
        if (value == null) {
            addCacheMiss();
        } else {
            addCacheHit();
            if (value.getClass().getName().equals(NullValue.class.getName())) {
                return null;
            }
            if (type != null && !type.isInstance(value)) {
                throw new IllegalStateException("Cached value is not of required type [" + type.getName() + "]: " + value);
            }
        }
        return (T) fromStoreValue(value);
    }

    @Override
    public void put(Object key, Object value) {
        if (!allowNullValues && value == null) {
            mapCache.remove(key);
            return;
        }

        value = toStoreValue(value);
        mapCache.fastPut(key, value);
        addCachePut();
    }

    public ValueWrapper putIfAbsent(Object key, Object value) {
        Object prevValue;
        if (!allowNullValues && value == null) {
            prevValue = mapCache.get(key);
        } else {
            value = toStoreValue(value);
            prevValue = mapCache.putIfAbsent(key, value);
            if (prevValue == null) {
                addCachePut();
            }
        }

        return toValueWrapper(prevValue);
    }

    @Override
    public void evict(Object key) {
        mapCache.fastRemove(key);
    }

    @Override
    public void clear() {
        mapCache.clear();
    }

    private ValueWrapper toValueWrapper(Object value) {
        if (value == null) {
            return null;
        }
        if (value.getClass().getName().equals(NullValue.class.getName())) {
            return NullValue.INSTANCE;
        }
        return new SimpleValueWrapper(value);
    }

    public <T> T get(Object key, Callable<T> valueLoader) {
        Object value = mapCache.get(key);
        if (value == null) {
            addCacheMiss();
            RLock lock = mapCache.getLock(key);
            lock.lock();
            try {
                value = mapCache.get(key);
                if (value == null) {
                    value = putValue(key, valueLoader, value);
                }
            } finally {
                lock.unlock();
            }
        } else {
            addCacheHit();
        }

        return (T) fromStoreValue(value);
    }

    private <T> Object putValue(Object key, Callable<T> valueLoader, Object value) {
        try {
            value = valueLoader.call();
        } catch (Exception ex) {
            RuntimeException exception;
            try {
                Class<?> c = Class.forName("org.springframework.cache.Cache$ValueRetrievalException");
                Constructor<?> constructor = c.getConstructor(Object.class, Callable.class, Throwable.class);
                exception = (RuntimeException) constructor.newInstance(key, valueLoader, ex);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            throw exception;
        }
        put(key, value);
        return value;
    }

    protected Object fromStoreValue(Object storeValue) {
        if (storeValue instanceof NullValue) {
            return null;
        }
        return storeValue;
    }

    protected Object toStoreValue(Object userValue) {
        if (userValue == null) {
            return NullValue.INSTANCE;
        }
        return userValue;
    }


    long getCacheHits() {
        return hits.get();
    }

    long getCacheMisses() {
        return misses.get();
    }

    long getCachePuts() {
        return puts.get();
    }

    private void addCachePut() {
        puts.incrementAndGet();
    }

    private void addCacheHit() {
        hits.incrementAndGet();
    }

    private void addCacheMiss() {
        misses.incrementAndGet();
    }

}
