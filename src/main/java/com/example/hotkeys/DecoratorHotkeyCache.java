package com.example.hotkeys;

import org.springframework.cache.Cache;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.lang.Nullable;

import java.util.concurrent.Callable;

/**
 * 装饰 `Cache`，在 `get` 操作时触发 `KeyEvent` 事件
 */
public class DecoratorHotkeyCache implements Cache {
    private final Cache delegate;  // 被装饰的 Cache
    private final ApplicationEventPublisher eventPublisher;  // 事件发布器

    public DecoratorHotkeyCache(Cache delegate, ApplicationEventPublisher eventPublisher) {
        this.delegate = delegate;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public Object getNativeCache() {
        return delegate;
    }

    @Override
    @Nullable
    public ValueWrapper get(Object key) {
        pushKeyEvent(key); // 触发事件
        return delegate.get(key);
    }

    @Override
    @Nullable
    public <T> T get(Object key, @Nullable Class<T> type) {
        pushKeyEvent(key); // 触发事件
        return delegate.get(key, type);
    }

    @Override
    @Nullable
    public <T> T get(Object key, Callable<T> valueLoader) {
        pushKeyEvent(key); // 触发事件
        return delegate.get(key, valueLoader);
    }

    @Override
    public void put(Object key, @Nullable Object value) {
        delegate.put(key, value);
    }

    @Override
    public void evict(Object key) {
        delegate.evict(key);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    private void pushKeyEvent(Object key) {
        eventPublisher.publishEvent(new KeyEvent(key.toString()));
    }
}
