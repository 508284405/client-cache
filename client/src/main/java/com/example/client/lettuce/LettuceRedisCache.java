package com.example.client.lettuce;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.support.caching.CacheFrontend;
import org.springframework.cache.support.AbstractValueAdaptingCache;

import java.util.concurrent.Callable;

public class LettuceRedisCache extends AbstractValueAdaptingCache {
    private final String name;
    private CacheFrontend<String, Object> cacheFrontend;
    private final StatefulRedisConnection<String, Object> connection;

    protected LettuceRedisCache(boolean allowNullValues, String name, StatefulRedisConnection connection) {
        super(allowNullValues);
        this.name = name;
        this.connection = connection;
    }

    public void setCacheFrontend(CacheFrontend<String,Object> cacheFrontend){
        this.cacheFrontend = cacheFrontend;
    }

    @Override
    protected Object lookup(Object key) {
        if (cacheFrontend != null)
            return cacheFrontend.get((String) key);
        return connection.sync().get((String) key);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object getNativeCache() {
        return cacheFrontend;
    }

    @Override
    public <T> T get(Object key, Callable<T> valueLoader) {
        ValueWrapper result = get(key);

        if (result != null) {
            return (T) result.get();
        }

        return getSynchronized(key, valueLoader);
    }

    private synchronized <T> T getSynchronized(Object key, Callable<T> valueLoader) {

        ValueWrapper result = get(key);

        if (result != null) {
            return (T) result.get();
        }

        T value;
        try {
            value = valueLoader.call();
        } catch (Exception e) {
            throw new ValueRetrievalException(key, valueLoader, e);
        }
        put(key, value);
        return value;
    }

    @Override
    public void put(Object key, Object value) {
        connection.sync().set((String) key,value);
    }

    @Override
    public void evict(Object key) {
        connection.sync().del((String) key);
    }

    @Override
    public void clear() {

    }
}
