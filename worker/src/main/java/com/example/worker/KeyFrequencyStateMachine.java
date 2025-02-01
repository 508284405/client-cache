package com.example.worker;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.example.worker.util.FilePersistenceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class KeyFrequencyStateMachine implements StateMachine {
    private static final Logger LOG = LoggerFactory.getLogger(KeyFrequencyStateMachine.class);

    // key -> 访问时间队列
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>> freqMap = new ConcurrentHashMap<>();

    // 滑动窗口时间（毫秒）
    private static final long WINDOW_SIZE_MS = 300000;

    @Override
    public void onApply(Iterator iter) {
        while (iter.hasNext()) {
            ByteBuffer data = iter.getData();
            if (data != null) {
                // 反序列化data为ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>>，并与freqMap合并; 更新本地计数
                mergeWithFreqMap(data,freqMap);
            }
            iter.next();
        }
    }


    /**
     * 反序列化 ByteBuffer 为 ConcurrentHashMap 并与 freqMap 合并
     */
    private void mergeWithFreqMap(ByteBuffer buffer, ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>> freqMap) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(buffer.array());
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            // 反序列化数据
            @SuppressWarnings("unchecked")
            ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>> deserializedMap = (ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>>) ois.readObject();

            // 合并到 freqMap
            for (Map.Entry<String, ConcurrentLinkedQueue<Long>> entry : deserializedMap.entrySet()) {
                ConcurrentLinkedQueue<Long> queue = freqMap.computeIfAbsent(entry.getKey(), k -> new ConcurrentLinkedQueue<>());
                queue.addAll(entry.getValue());  // 合并 Queue<Long> 到 freqMap
                long currentTime = System.currentTimeMillis();
                cleanUp(entry.getKey(), currentTime);
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to deserialize and merge freqMap", e);
        }
    }

    @Override
    public void onShutdown() {
        LOG.info("shutdown");
    }

    // 快照保存
    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        try {
            String snapshotPath = writer.getPath() + "/freqMap.snapshot";
            FilePersistenceUtil.saveFreqMapToFile(freqMap, snapshotPath);
            done.run(Status.OK());
        } catch (Exception e) {
            LOG.error("Snapshot save failed", e);
            done.run(new Status(-1, "Snapshot save failed: " + e.getMessage()));
        }
    }

    // 快照加载
    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        try {
            String snapshotPath = reader.getPath() + "/freqMap.snapshot";
            ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>> loaded = FilePersistenceUtil.loadFreqMapFromFile(snapshotPath);
            if (loaded != null) {
                freqMap.clear();
                freqMap.putAll(loaded);
                LOG.info("Loaded freqMap from snapshot, size={}", freqMap.size());
            }
        } catch (Exception e) {
            LOG.error("Snapshot load failed", e);
        }
        return true;
    }

    @Override
    public void onLeaderStart(long term) {
        LOG.info("Become Leader, term={}", term);
    }

    @Override
    public void onLeaderStop(Status status) {
        LOG.info("Step down from Leader: {}", status);
    }

    @Override
    public void onError(RaftException e) {
        LOG.error("Raft error: {}", e.getMessage(), e);
    }

    @Override
    public void onConfigurationCommitted(Configuration conf) {
        LOG.info("Configuration committed: {}", conf);
    }

    @Override
    public void onStopFollowing(LeaderChangeContext context) {
        LOG.info("Stop following {}", context.getLeaderId());
    }

    @Override
    public void onStartFollowing(LeaderChangeContext context) {
        LOG.info("Start following {}", context.getLeaderId());
    }

    /**
     * 手动更新Key次数(用于Follower本地临时累计)
     * @param key key
     */
    public void applyKeyLocally(String key) {
        long currentTime = System.currentTimeMillis();
        freqMap.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>()).add(currentTime);
        cleanUp(key, currentTime);
    }

    /**
     * 清理窗口外的数据
     */
    private void cleanUp(String key, long currentTime) {
        ConcurrentLinkedQueue<Long> queue = freqMap.get(key);
        if (queue == null) return;

        while (!queue.isEmpty() && queue.peek() < (currentTime - WINDOW_SIZE_MS)) {
            queue.poll(); // 移除过期数据
        }
    }

    /**
     * 获取当前窗口内的访问次数
     */
    public int getFrequency(String key) {
        ConcurrentLinkedQueue<Long> queue = freqMap.get(key);
        return queue == null ? 0 : queue.size();
    }

    public ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>> getFreqMap() {
        return freqMap;
    }
}