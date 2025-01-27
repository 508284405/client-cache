package com.example.client.lettuce;

import com.example.client.hotkeys.DecoratorHotkeyCache;
import com.example.client.hotkeys.HotKeyEvent;
import io.lettuce.core.support.caching.CacheFrontend;
import org.springframework.cache.Cache;
import org.springframework.context.ApplicationListener;


public class LettuceHotKeyListener implements ApplicationListener<HotKeyEvent> {
    private final LettuceCacheManager lettuceCacheManager;

    public LettuceHotKeyListener(LettuceCacheManager lettuceCacheManager) {
        this.lettuceCacheManager = lettuceCacheManager;
    }

    @Override
    public void onApplicationEvent(HotKeyEvent event) {
        //
        String key = (String) event.getSource();
        String namespace = key.substring(0, key.indexOf("::"));
        Cache cache = lettuceCacheManager.getCache(namespace);
        if (cache == null) {
            return;
        }
        if (cache instanceof DecoratorHotkeyCache) {
            Object nativeCache = cache.getNativeCache();
            if (nativeCache instanceof LettuceRedisCache) {
                LettuceRedisCache redisCache = (LettuceRedisCache) nativeCache;
                if (redisCache.getNativeCache() instanceof CacheFrontend) {
                    // 已经开启了本地缓存
                    return;
                }
                CacheFrontend<String, Object> cacheFrontend = lettuceCacheManager.createCacheFrontend(namespace);
                redisCache.setCacheFrontend(cacheFrontend);
            }
        }
    }
}
