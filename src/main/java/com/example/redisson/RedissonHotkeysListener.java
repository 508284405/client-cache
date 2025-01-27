package com.example.redisson;

import com.example.hotkeys.DecoratorHotkeyCache;
import com.example.hotkeys.HotKeyEvent;
import org.redisson.RedissonLocalCachedMap;
import org.slf4j.Logger;
import org.springframework.cache.Cache;
import org.springframework.context.ApplicationListener;

public class RedissonHotkeysListener implements ApplicationListener<HotKeyEvent> {
    private final Logger logger = org.slf4j.LoggerFactory.getLogger(RedissonHotkeysListener.class);
    private final RedissonCacheManager cacheManager;

    public RedissonHotkeysListener(RedissonCacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Override
    public void onApplicationEvent(HotKeyEvent event) {
        // 接收到热点key以后，进行本地缓存处理
        String key = (String) event.getSource();
        String namespace = key.substring(0, key.indexOf("::"));
        Cache cache = cacheManager.getCache(namespace);
        if (cache instanceof DecoratorHotkeyCache) {
            if (cache.getNativeCache() instanceof RedissonLocalCache) {
                // 已经开启了本地缓存
                logger.debug("{} 已经开启了本地缓存", namespace);
                return;
            }
            // 开启本地缓存
            cacheManager.enableRLocalCachedMap(namespace);
        }
    }
}
