package com.example.worker.netty;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.Status;
import com.example.worker.JRaftServerHolder;
import com.example.worker.JRaftServer;
import com.example.worker.KeyFrequencyStateMachine;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class WorkerNettyHandler extends SimpleChannelInboundHandler<String> {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerNettyHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        LOG.info("Received key from client: {}", msg);

        JRaftServer raftServer = JRaftServerHolder.getServer();
        if (raftServer.isLeader()) {
            // 只有Leader才进行日志复制
            Task task = new Task();
            task.setData(ByteBuffer.wrap(msg.getBytes(StandardCharsets.UTF_8)));
            task.setDone(new Closure() {
                @Override
                public void run(Status status) {
                    if (!status.isOk()) {
                        LOG.warn("Apply key failed, {}", status);
                    }
                }
            });
            raftServer.getNode().apply(task);
        } else {
            // Follower只在本地记录一次，后续定时上报给Leader
            KeyFrequencyStateMachine fsm = raftServer.getFsm();
            fsm.applyKeyLocally(msg);
        }

        // 给客户端回复
        ctx.writeAndFlush("ACK: " + msg);
    }
}