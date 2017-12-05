package ict.ada.gdb.proxy;


import ict.ada.gdb.redis.consistent.HashTable;
import ict.ada.gdb.redis.consistent.RedisProxy;
import ict.ada.gdb.redis.consistent.resources.RedisData;
import ict.ada.gdb.redis.consistent.server.RedisServer;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by lon on 17-4-16.
 */
public class RedisProxyConsistent implements RedisProxy {
    // get one instance for hashtable
    //private static Jedis hashTableProxy = new Jedis("10.100.1.35");
    private RedisServer server = new RedisServer();


    public Map<String, String> hgetAll(String key, int db) {
        return server.hgetAll(key, db);
    }

    public void hset(String key, String field, String value, int db) {
        server.hset(new RedisData(key, value, HashTable.getColor(key)), field, db);
    }

    public boolean hexists(String key, String field, int db) {
        return server.hexists(key, field, db);
    }

    public String hget(String key, String field, int db) {
        return server.hget(key, field, db);
    }

    public void lpush(String key, int db, String... value) {
        server.lpush(key, value, db);
    }

    public List<String> lrange(String key, long start, long end, int db) {

        return server.lrange(key, start, end, db);
    }

    public void del(String key, int db) {
        try {
            server.deleteData(key, db);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void flushDB(int db) {
        server.flushDB(db);
    }

    public Set<String> keys(String s, int db) {
        return server.keys(s, db);
    }

    public void addNodeToCluster(String ipAddress, int port, int noOfReplicas) {
        try {
            server.addNodeToCluster(ipAddress, port, noOfReplicas);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
