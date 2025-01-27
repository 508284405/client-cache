package com.example.client.redisson;

import com.example.client.hotkeys.DecoratorHotkeyCache;
import org.redisson.api.*;
import org.redisson.client.codec.Codec;
import org.redisson.spring.cache.CacheConfig;
import org.redisson.spring.cache.RedissonCache;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.transaction.TransactionAwareCacheDecorator;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 从org.redisson.spring.cache.RedissonSpringCacheManager 改造而来
 */
public class RedissonCacheManager implements CacheManager, ResourceLoaderAware, InitializingBean {

    ResourceLoader resourceLoader;

    private boolean dynamic = true;

    private boolean allowNullValues = true;

    private boolean transactionAware = false;

    private boolean hotkeysMonitor = true;

    private final ApplicationEventPublisher eventPublisher;

    Codec codec;

    RedissonClient redisson;

    Map<String, CacheConfig> configMap = new ConcurrentHashMap<String, CacheConfig>();
    ConcurrentMap<String, Cache> instanceMap = new ConcurrentHashMap<String, Cache>();

    String configLocation;


    public RedissonCacheManager(RedissonClient redisson, ApplicationEventPublisher eventPublisher) {
        this(eventPublisher, redisson, (String) null, null);
    }


    public RedissonCacheManager(RedissonClient redisson, Map<String, ? extends CacheConfig> config, ApplicationEventPublisher eventPublisher) {
        this(eventPublisher, redisson, config, null);
    }

    public RedissonCacheManager(ApplicationEventPublisher eventPublisher, RedissonClient redisson, Map<String, ? extends CacheConfig> config, Codec codec) {
        this.eventPublisher = eventPublisher;
        this.redisson = redisson;
        this.configMap = (Map<String, CacheConfig>) config;
        this.codec = codec;
    }


    public RedissonCacheManager(RedissonClient redisson, String configLocation, ApplicationEventPublisher eventPublisher) {
        this(eventPublisher, redisson, configLocation, null);
    }


    public RedissonCacheManager(ApplicationEventPublisher eventPublisher, RedissonClient redisson, String configLocation, Codec codec) {
        this.eventPublisher = eventPublisher;
        this.redisson = redisson;
        this.configLocation = configLocation;
        this.codec = codec;
    }


    public void setAllowNullValues(boolean allowNullValues) {
        this.allowNullValues = allowNullValues;
    }

    public void setTransactionAware(boolean transactionAware) {
        this.transactionAware = transactionAware;
    }

    public void setCacheNames(Collection<String> names) {
        if (names != null) {
            for (String name : names) {
                getCache(name);
            }
            dynamic = false;
        } else {
            dynamic = true;
        }
    }

    public void setConfigLocation(String configLocation) {
        this.configLocation = configLocation;
    }


    public void setConfig(Map<String, ? extends CacheConfig> config) {
        this.configMap = (Map<String, CacheConfig>) config;
    }

    public void setRedisson(RedissonClient redisson) {
        this.redisson = redisson;
    }


    public void setCodec(Codec codec) {
        this.codec = codec;
    }

    protected CacheConfig createDefaultConfig() {
        return new CacheConfig();
    }

    @Override
    public Cache getCache(String name) {
        Cache cache = instanceMap.get(name);
        if (cache != null) {
            return cache;
        }
        if (!dynamic) {
            return cache;
        }

        CacheConfig config = configMap.get(name);
        if (config == null) {
            config = createDefaultConfig();
            configMap.put(name, config);
        }

        if (config.getMaxIdleTime() == 0 && config.getTTL() == 0 && config.getMaxSize() == 0) {
            return createMap(name, config);
        }

        return createMapCache(name, config);
    }

    private Cache createMap(String name, CacheConfig config) {
        RMap<Object, Object> map = getMap(name, config);

        Cache cache = new RedissonCache(map, allowNullValues);
        if (transactionAware) {
            cache = new TransactionAwareCacheDecorator(cache);
        }
        if (hotkeysMonitor) {
            cache = new DecoratorHotkeyCache(cache, eventPublisher);
        }
        Cache oldCache = instanceMap.putIfAbsent(name, cache);
        if (oldCache != null) {
            cache = oldCache;
        }
        return cache;
    }

    protected RMap<Object, Object> getMap(String name, CacheConfig config) {
        if (codec != null) {
            return redisson.getMap(name, codec);
        }
        return redisson.getMap(name);
    }

    private Cache createMapCache(String name, CacheConfig config) {
        RMapCache<Object, Object> map = getMapCache(name, config);

        Cache cache = new RedissonCache(map, config, allowNullValues);
        if (transactionAware) {
            cache = new TransactionAwareCacheDecorator(cache);
        }
        if (hotkeysMonitor) {
            cache = new DecoratorHotkeyCache(cache, eventPublisher);
        }
        Cache oldCache = instanceMap.putIfAbsent(name, cache);
        if (oldCache != null) {
            cache = oldCache;
        } else {
            map.setMaxSize(config.getMaxSize());
        }
        return cache;
    }

    protected RMapCache<Object, Object> getMapCache(String name, CacheConfig config) {
        if (codec != null) {
            return redisson.getMapCache(name, codec);
        }
        return redisson.getMapCache(name);
    }

    @Override
    public Collection<String> getCacheNames() {
        return Collections.unmodifiableSet(configMap.keySet());
    }

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (configLocation == null) {
            return;
        }

        Resource resource = resourceLoader.getResource(configLocation);
        try {
            this.configMap = (Map<String, CacheConfig>) CacheConfig.fromJSON(resource.getInputStream());
        } catch (IOException e) {
            // try to read yaml
            try {
                this.configMap = (Map<String, CacheConfig>) CacheConfig.fromYAML(resource.getInputStream());
            } catch (IOException e1) {
                throw new BeanDefinitionStoreException(
                        "Could not parse cache configuration at [" + configLocation + "]", e1);
            }
        }
    }

    protected synchronized void enableRLocalCachedMap(String namespace) {
        Cache cache = this.instanceMap.get(namespace);
        if (cache instanceof DecoratorHotkeyCache && cache.getNativeCache() instanceof RedissonLocalCache) {
            // 该namespace已经开启本地缓存
            return;
        }
        // 构建本地缓存
        LocalCachedMapOptions<Object, Object> localCachedMapOptions = LocalCachedMapOptions.<Object, Object>defaults()
                .cacheProvider(LocalCachedMapOptions.CacheProvider.REDISSON)
                .evictionPolicy(LocalCachedMapOptions.EvictionPolicy.LRU)
                .storeMode(LocalCachedMapOptions.StoreMode.LOCALCACHE_REDIS)
                .syncStrategy(LocalCachedMapOptions.SyncStrategy.INVALIDATE);
        Optional.ofNullable(this.configMap.get(namespace)).ifPresent(cacheConfig -> {
            localCachedMapOptions.cacheSize(cacheConfig.getMaxSize());
            localCachedMapOptions.timeToLive(cacheConfig.getTTL());
            localCachedMapOptions.maxIdle(cacheConfig.getMaxIdleTime());
        });
        RLocalCachedMap<Object, Object> localCachedMap = null;
        if (codec != null) {
            localCachedMap = redisson.getLocalCachedMap(namespace, codec, localCachedMapOptions);
        } else {
            localCachedMap = redisson.getLocalCachedMap(namespace, localCachedMapOptions);
        }
        this.instanceMap.put(namespace, new DecoratorHotkeyCache(new RedissonLocalCache(localCachedMap, allowNullValues), eventPublisher));
    }
}
