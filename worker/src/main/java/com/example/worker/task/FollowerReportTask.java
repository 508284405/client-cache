package com.example.worker.task;

import com.alipay.sofa.jraft.entity.Task;
import com.example.worker.raft.JRaftServer;
import com.example.worker.raft.JRaftServerHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Follower -> Leader 上报任务
 */
@Component
public class FollowerReportTask {
    private static final Logger LOG = LoggerFactory.getLogger(FollowerReportTask.class);
    private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final JRaftServerHolder jRaftServerHolder;

    public FollowerReportTask(JRaftServerHolder jRaftServerHolder) {
        this.jRaftServerHolder = jRaftServerHolder;
    }

    /**
     * 启动Follower->Leader上报任务 & Leader/Follower持久化任务
     */
    @PostConstruct
    public void startSchedule() {
        executor.scheduleAtFixedRate(() -> {
            try {
                JRaftServer server = jRaftServerHolder.getServer();
                if (server == null) return;

                // 如果自己是Follower，就把 freqMap 数据上报给Leader
                if (!server.isLeader()) {
                    Map<String, ConcurrentLinkedQueue<Long>> freqMap = server.getFsm().getFreqMap();
                    freqMap.forEach((key, timeList) -> {
                        // 构造 Task
                        Task task = new Task();
                        // 把整个freqMap的值序列化成ByteBuffer
                        task.setData(serializeFreqMap(freqMap));
                        task.setDone(status -> {
                            if (!status.isOk()) {
                                LOG.error("Follower report key={} failed: {}", key, status);
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

    /**
     * 使用 Java 对象流序列化 freqMap
     */
    private static ByteBuffer serializeFreqMap(Map<String, ConcurrentLinkedQueue<Long>> freqMap) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(freqMap);
            oos.flush();
            return ByteBuffer.wrap(bos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize freqMap", e);
        }
    }
}