package com.example.worker;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class NettyClientTest {
    private static final Logger LOG = LoggerFactory.getLogger(NettyClientTest.class);

    private final String host;
    private final int port;
    private Channel channel;
    private EventLoopGroup group;

    public NettyClientTest(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws InterruptedException {
        group = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap();
        b.group(group)
         .channel(NioSocketChannel.class)
         .handler(new ChannelInitializer<Channel>() {
             @Override
             protected void initChannel(Channel ch) throws Exception {
                 ch.pipeline().addLast(new StringDecoder(StandardCharsets.UTF_8));
                 ch.pipeline().addLast(new StringEncoder(StandardCharsets.UTF_8));
             }
         });

        ChannelFuture future = b.connect(host, port).sync();
        if (future.isSuccess()) {
            channel = future.channel();
            LOG.info("Client connected to {}:{}", host, port);
        }
    }

    public void sendKey(String key) {
        if (channel != null) {
            LOG.info("Sending key to worker: {}", key);
            channel.writeAndFlush(key);
        }
    }

    public void close() {
        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
    }
}