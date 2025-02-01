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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

public class KeyFrequencyStateMachine implements StateMachine {
    private static final Logger LOG = LoggerFactory.getLogger(KeyFrequencyStateMachine.class);

    // 维护Key->访问次数
    private final ConcurrentHashMap<String, Long> freqMap = new ConcurrentHashMap<>();

    @Override
    public void onApply(Iterator iter) {
        while (iter.hasNext()) {
            ByteBuffer data = iter.getData();
            if (data != null) {
                String key = new String(data.array(), StandardCharsets.UTF_8);
                // 更新本地计数
                freqMap.merge(key, 1L, Long::sum);
            }
            iter.next();
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
            ConcurrentHashMap<String, Long> loaded = FilePersistenceUtil.loadFreqMapFromFile(snapshotPath);
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

    // 手动更新Key次数(用于Follower本地临时累计)
    public void applyKeyLocally(String key) {
        freqMap.merge(key, 1L, Long::sum);
    }

    public ConcurrentHashMap<String, Long> getFreqMap() {
        return freqMap;
    }
}