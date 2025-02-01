package com.example.worker;

import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.entity.PeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JRaftServer {
    private static final Logger LOG = LoggerFactory.getLogger(JRaftServer.class);

    private RaftGroupService raftGroupService;
    private Node node;
    private KeyFrequencyStateMachine fsm;

    public void start(String dataPath, String groupId, String serverId, String initialConf) {
        // 1. 创建NodeOptions
        NodeOptions nodeOptions = new NodeOptions();
        // 选举超时时间
        nodeOptions.setElectionTimeoutMs(5000);
        // 每30秒做一次snapshot
        nodeOptions.setSnapshotIntervalSecs(30);

        // 2. 创建状态机
        this.fsm = new KeyFrequencyStateMachine();
        nodeOptions.setFsm(this.fsm);

        // 3. 日志、元数据和快照存储路径（可灵活配置）
        nodeOptions.setLogUri(dataPath + "/log");
        nodeOptions.setRaftMetaUri(dataPath + "/raft_meta");
        nodeOptions.setSnapshotUri(dataPath + "/snapshot");

        // 4. 集群配置
        Configuration conf = new Configuration();
        if (!conf.parse(initialConf)) {
            LOG.error("Fail to parse conf: {}", initialConf);
            return;
        }
        nodeOptions.setInitialConf(conf);

        // 5. 解析自身的serverId
        PeerId peerId = new PeerId();
        if (!peerId.parse(serverId)) {
            LOG.error("Fail to parse serverId: {}", serverId);
            return;
        }

        // 6. 启动RaftGroupService
        this.raftGroupService = new RaftGroupService(groupId, peerId, nodeOptions);
        this.node = this.raftGroupService.start();
        LOG.info("JRaftServer started with serverId={}, groupId={}, conf={}", serverId, groupId, initialConf);
    }

    public boolean isLeader() {
        return node != null && node.isLeader();
    }

    public Node getNode() {
        return node;
    }

    public KeyFrequencyStateMachine getFsm() {
        return fsm;
    }

    public void shutdown() {
        if (raftGroupService != null) {
            raftGroupService.shutdown();
        }
    }
}