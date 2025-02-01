package com.example.worker;

public class JRaftServerHolder {
    private static JRaftServer server;

    public static void init(String dataPath, String groupId, String serverId, String initialConf) {
        server = new JRaftServer();
        server.start(dataPath, groupId, serverId, initialConf);
    }

    public static JRaftServer getServer() {
        return server;
    }
}