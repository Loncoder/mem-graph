package ict.ada.gdb.redis.consistent.main;

import ict.ada.gdb.redis.consistent.json.KeyDetailsJSON;
import ict.ada.gdb.redis.consistent.resources.Node;
import ict.ada.gdb.redis.consistent.resources.RedisData;
import ict.ada.gdb.redis.consistent.server.RedisServer;

import java.util.List;

/**
 * Created by lon on 17-4-16.
 */
public class RedisClusterTest {
    public static void main(String[] args) throws Exception {

        RedisServer redisServer = new RedisServer();
        System.out.println("Add node to cluster ...");
        System.out.println("begin add node 10.100.1.35:6379");
        redisServer.addNodeToCluster("10.100.1.35", 6379, 2);

        System.out.println("begin add node 10.100.1.37:6379");
        redisServer.addNodeToCluster("10.100.1.37", 6379, 2);

        System.out.println("begin add node 10.100.1.38:6379");
        redisServer.addNodeToCluster("10.100.1.38", 6379, 2);

        System.out.println("insert 100 data");
        for (int i = 0; i < 100; i++) {
            RedisData data = new RedisData("key-" + i, i);
            redisServer.insertData(data);
        }
        System.out.println("begin add node 10.100.1.39:6379");
        List<KeyDetailsJSON> migratedKeys = redisServer.addNodeToCluster("10.100.1.39", 6379, 2);
        System.out.println("migrate keys ....");
        for (KeyDetailsJSON json : migratedKeys)
            System.out.println(json);


        Node node35 = new Node("10.100.1.35", 6379);
        List<RedisData> redisData35 = redisServer.getDataFromNode(node35);
        System.out.println(redisData35);
        System.out.println("redisData35 size : " + redisData35.size());

        Node node37 = new Node("10.100.1.37", 6379);
        List<RedisData> redisData37 = redisServer.getDataFromNode(node37);
        System.out.println(redisData37);
        System.out.println("redisData37 size : " + redisData37.size());

        Node node38 = new Node("10.100.1.38", 6379);
        List<RedisData> redisData38 = redisServer.getDataFromNode(node38);
        System.out.println(redisData38);
        System.out.println("redisData38 size : " + redisData38.size());

        Node node39 = new Node("10.100.1.39", 6379);
        List<RedisData> redisData39 = redisServer.getDataFromNode(node39);
        System.out.println(redisData39);
        System.out.println("redisData39 size : " + redisData39.size());
    }
}
