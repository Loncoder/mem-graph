package ict.ada.gdb.redis.consistent;

import ict.ada.gdb.redis.consistent.server.RedisServer;

/**
 * Created by lon on 17-4-19.
 */
public class RedisMain {
    public static void main(String[] args) throws Exception {
        RedisServer server = new RedisServer();
        server.addNodeToCluster("10.100.1.35", 6379, 1);
        server.addNodeToCluster("10.100.1.37", 6379, 1);
        server.addNodeToCluster("10.100.1.38", 6379, 1);
        server.addNodeToCluster("10.100.1.39", 6379, 1);
        server.addNodeToCluster("10.100.1.40", 6379, 1);
        server.addNodeToCluster("10.100.1.41", 6379, 1);
    }
}
