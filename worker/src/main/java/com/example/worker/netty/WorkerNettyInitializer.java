package com.example.worker.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class WorkerNettyInitializer extends ChannelInitializer<SocketChannel> {

    private final WorkerNettyHandler workerNettyHandler;

    public WorkerNettyInitializer(WorkerNettyHandler workerNettyHandler) {
        this.workerNettyHandler = workerNettyHandler;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline().addLast(new StringDecoder(StandardCharsets.UTF_8));
        ch.pipeline().addLast(new StringEncoder(StandardCharsets.UTF_8));
        ch.pipeline().addLast(workerNettyHandler);
    }
}