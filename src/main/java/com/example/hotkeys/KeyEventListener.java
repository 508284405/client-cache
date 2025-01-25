package com.example.hotkeys;

import org.slf4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class KeyEventListener implements ApplicationListener<KeyEvent> {
    private final Logger log = org.slf4j.LoggerFactory.getLogger(KeyEventListener.class);
    private final StringRedisTemplate stringRedisTemplate;
    private static final long TIME_WINDOW = 60; // 60秒窗口
    private static final int HOT_KEY_THRESHOLD = 10; // 访问次数阈值
    private final ApplicationEventPublisher eventPublisher;


    public KeyEventListener(StringRedisTemplate stringRedisTemplate, ApplicationEventPublisher eventPublisher) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void onApplicationEvent(KeyEvent event) {
        String key = (String) event.getSource();
        log.info("key: {}", key);

        // 滑动窗口计算每分钟内10key访问为热点key
        // 获取 Redis ZSet 操作对象
        ZSetOperations<String, String> zSetOps = stringRedisTemplate.opsForZSet();
        long currentTime = Instant.now().getEpochSecond(); // 当前时间戳（秒）
        // 1. 添加当前访问时间戳到 Redis ZSet
        zSetOps.add(key, String.valueOf(currentTime), currentTime);
        // 2. 移除超过滑动窗口范围的数据（超过 60 秒的时间戳）
        zSetOps.removeRangeByScore(key, 0, currentTime - TIME_WINDOW);
        // 3. 获取当前窗口内的访问次数
        Long count = zSetOps.zCard(key);
        log.info("key {}, count: {}", key,count);
        // 4. 判断是否达到热点阈值
        if (count != null && count >= HOT_KEY_THRESHOLD) {
            log.warn("热点 Key 发现: {}", key);
            eventPublisher.publishEvent(new HotKeyEvent(key));
        }
    }
}
