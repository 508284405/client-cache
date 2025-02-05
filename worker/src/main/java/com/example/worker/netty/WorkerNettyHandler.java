package com.example.worker.netty;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import com.example.worker.raft.JRaftServer;
import com.example.worker.raft.JRaftServerHolder;
import com.example.worker.raft.KeyFrequencyStateMachine;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Component
public class WorkerNettyHandler extends SimpleChannelInboundHandler<String> {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerNettyHandler.class);
    private final JRaftServerHolder jRaftServerHolder;

    public WorkerNettyHandler(JRaftServerHolder jRaftServerHolder) {
        this.jRaftServerHolder = jRaftServerHolder;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) {
        LOG.info("Received key from client: {}", msg);

        JRaftServer raftServer = jRaftServerHolder.getServer();
        if (raftServer.isLeader()) {
            // 只有Leader才进行日志复制
            Task task = new Task();
            task.setData(ByteBuffer.wrap(msg.getBytes(StandardCharsets.UTF_8)));
            task.setDone(status -> {
                if (!status.isOk()) {
                    LOG.warn("Apply key failed, {}", status);
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