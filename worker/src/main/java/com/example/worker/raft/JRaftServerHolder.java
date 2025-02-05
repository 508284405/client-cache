package com.example.worker.raft;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class JRaftServerHolder {
    private static JRaftServer server;

    private @Value("${dataPath}") String dataPath;
    private @Value("${groupId}") String groupId;
    private @Value("${serverId}") String serverId;
    private @Value("${initialConf}") String initialConf;

    @PostConstruct
    public void init() {
        server = new JRaftServer();
        server.start(dataPath, groupId, serverId, initialConf);
    }

    public JRaftServer getServer() {
        return server;
    }
}