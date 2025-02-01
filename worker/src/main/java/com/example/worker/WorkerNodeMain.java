package com.example.worker;

import com.example.worker.netty.WorkerNettyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 启动脚本示例：
 *   java -jar worker-node.jar
 *     --serverId=127.0.0.1:8081
 *     --dataPath=./data1
 *     --groupId=hotspotGroup
 *     --initialConf=127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083
 *     --nettyPort=9001
 */
public class WorkerNodeMain {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerNodeMain.class);

    public static void main(String[] args) throws Exception {
        // 简化：使用System.getProperty或解析args来获取参数
        String serverId    = System.getProperty("serverId", "127.0.0.1:8082");
        String dataPath    = System.getProperty("dataPath", "/Users/wangyu/App/javaproduct/spring-cache-client/worker/data2");
        String groupId     = System.getProperty("groupId", "hotspotGroup");
        String initialConf = System.getProperty("initialConf", "127.0.0.1:8081,127.0.0.1:8082");
        int nettyPort      = Integer.parseInt(System.getProperty("nettyPort", "9002"));

        LOG.info("Starting WorkerNode with serverId={}, dataPath={}, groupId={}, conf={}, nettyPort={}",
                serverId, dataPath, groupId, initialConf, nettyPort);

        // 1. 启动JRaft
        JRaftServerHolder.init(dataPath, groupId, serverId, initialConf);

        // 2. 启动Netty服务
        WorkerNettyServer nettyServer = new WorkerNettyServer(nettyPort);
        nettyServer.start();

        // 3. 启动Follower->Leader上报任务 & Leader/Follower持久化任务
        FollowerReportTask.startSchedule();
        
        LOG.info("WorkerNodeMain started successfully.");
    }
}