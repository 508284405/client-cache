package com.example.worker.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.nio.charset.StandardCharsets;

public class WorkerNettyInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new StringDecoder(StandardCharsets.UTF_8));
        ch.pipeline().addLast(new StringEncoder(StandardCharsets.UTF_8));
        ch.pipeline().addLast(new WorkerNettyHandler());
    }
}