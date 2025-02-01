package com.example.worker.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerNettyServer {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerNettyServer.class);

    private final int port;
    private ChannelFuture channelFuture;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public WorkerNettyServer(int port) {
        this.port = port;
    }

    public void start() throws InterruptedException {
        bossGroup   = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .childHandler(new WorkerNettyInitializer());

        this.channelFuture = b.bind(port).sync();
        LOG.info("Worker Netty server started at port {}", port);
    }

    public void stop() {
        if (channelFuture != null) {
            channelFuture.channel().close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }
}