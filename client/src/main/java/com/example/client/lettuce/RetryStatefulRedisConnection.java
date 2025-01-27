package com.example.client.lettuce;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.push.PushListener;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.resource.ClientResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// 装饰者
@Component
@ConditionalOnBean(RedisClient.class)
public class RetryStatefulRedisConnection implements StatefulRedisConnection<String, Object>, SmartLifecycle {
    private final Logger log = LoggerFactory.getLogger(RetryStatefulRedisConnection.class);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    private StatefulRedisConnection<String, Object> connection;
    private final RedisClient redisClient;

    public RetryStatefulRedisConnection(RedisClient redisClient) {
        this.redisClient = redisClient;
    }


    @Override
    public void start() {
        if (connection == null || !connection.isOpen()) {
            connection = redisClient.connect(StringObjectRedisCodec.INSTANCE);
        }
        //
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            if (!connection.isOpen()) {
                connection = redisClient.connect(StringObjectRedisCodec.INSTANCE);
                log.info("redis connection reconnect");
            }
        }, 0, 3, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        scheduledExecutorService.shutdown();
        connection.close();
        redisClient.shutdown();
    }

    @Override
    public boolean isRunning() {
        return true;
    }


    @Override
    public void setTimeout(Duration timeout) {
        connection.setTimeout(timeout);
    }

    @Override
    public Duration getTimeout() {
        return connection.getTimeout();
    }

    @Override
    public Collection<RedisCommand> dispatch(Collection collection) {
        return connection.dispatch(collection);
    }

    @Override
    public RedisCommand dispatch(RedisCommand command) {
        return connection.dispatch(command);
    }

    @Override
    public void close() {
        connection.close();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return connection.closeAsync();
    }

    @Override
    public boolean isOpen() {
        return connection.isOpen();
    }

    @Override
    public ClientOptions getOptions() {
        return connection.getOptions();
    }

    @Override
    public ClientResources getResources() {
        return connection.getResources();
    }

    @Override
    public void reset() {
        connection.reset();
    }

    @Override
    public void setAutoFlushCommands(boolean autoFlush) {
        connection.setAutoFlushCommands(autoFlush);
    }

    @Override
    public void flushCommands() {
        connection.flushCommands();
    }

    @Override
    public boolean isMulti() {
        return connection.isMulti();
    }

    @Override
    public RedisCommands sync() {
        return connection.sync();
    }

    @Override
    public RedisAsyncCommands async() {
        return connection.async();
    }

    @Override
    public RedisReactiveCommands reactive() {
        return connection.reactive();
    }

    @Override
    public void addListener(PushListener listener) {
        connection.addListener(listener);
    }

    @Override
    public void removeListener(PushListener listener) {
        connection.removeListener(listener);
    }
}
