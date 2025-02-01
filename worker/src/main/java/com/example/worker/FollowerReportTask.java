package com.example.worker;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.*;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.entity.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Follower -> Leader 上报任务
 */
public class FollowerReportTask {
    private static final Logger LOG = LoggerFactory.getLogger(FollowerReportTask.class);
    private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public static void startSchedule() {
        executor.scheduleAtFixedRate(() -> {
            try {
                JRaftServer server = JRaftServerHolder.getServer();
                if (server == null) return;

                // 如果自己是Follower，就把 freqMap 数据上报给Leader
                if (!server.isLeader()) {
                    Map<String, Long> freqMap = server.getFsm().getFreqMap();
                    // 这里简单演示：把整个 freqMap 的 key 都上报
                    // 生产环境可考虑只上报最近新增/增量
                    freqMap.forEach((key, count) -> {
                        // 构造 Task
                        String data = key; // 仅上报key，Leader收到后自动+1
                        Task task = new Task();
                        task.setData(ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)));
                        task.setDone(new Closure() {
                            @Override
                            public void run(com.alipay.sofa.jraft.Status status) {
                                if (!status.isOk()) {
                                    LOG.error("Follower report key={} failed: {}", key, status);
                                }
                            }
                        });
                        // 通过 Leader 节点 apply
                        server.getNode().apply(task);
                    });
                }
            } catch (Exception e) {
                LOG.error("Follower report task error:", e);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }
}