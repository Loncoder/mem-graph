package ict.ada.gdb.proxy;

import ict.ada.gdb.hbase.constant.ConstantMR;
import ict.ada.gdb.redis.consistent.RedisProxy;
import ict.ada.gdb.service.RedisInstance;
import redis.clients.jedis.Jedis;

import java.util.*;

public class RedisProxyMemGraph implements RedisProxy {

    static List<Jedis> jediss = new ArrayList<>();

    private static Jedis jedis = RedisInstance.getInstance().getService();

    static {
        jedis.select(15);
        for (String host : ConstantMR.jedisHosts) {
            jediss.add(new Jedis(host, 6379, 1));
        }
    }

    private int clusterId(String nodeId) {
        if (!jedis.exists(nodeId)) {
            return -1;
        }
        return Integer.parseInt(jedis.get(nodeId));
    }


    public Map<String, String> hgetAll(String key, int db) {
        int clusterId = clusterId(key);
        if (clusterId == -1) return null;
        Jedis redis = jediss.get(clusterId);
        redis.select(db);
        return redis.hgetAll(key);
    }

    public void hset(String key, String field, String value, int db) {
        int clusterId = clusterId(key);
        if (clusterId == -1) return;
        Jedis redis = jediss.get(clusterId);
        redis.select(db);

        redis.hset(key, field, value);
    }

    public boolean hexists(String key, String field, int db) {
        int clusterId = clusterId(key);
        if (clusterId == -1) return false;
        Jedis redis = jediss.get(clusterId);
        redis.select(db);
        return redis.hexists(key, field);
    }

    public String hget(String key, String field, int db) {
        int clusterId = clusterId(key);
        if (clusterId == -1) return "";
        Jedis redis = jediss.get(clusterId);
        redis.select(db);
        return redis.hget(key, field);
    }

    public void lpush(String key, int db, String... value) {

        int clusterId = clusterId(key);
        if (clusterId == -1) return;
        Jedis redis = jediss.get(clusterId);
        redis.select(db);

        redis.lpush(key, value);
    }

    public List<String> lrange(String key, long start, long end, int db) {

        int clusterId = clusterId(key);
        if (clusterId == -1) return Collections.emptyList();
        Jedis redis = jediss.get(clusterId);
        redis.select(db);

        return redis.lrange(key, start, end);
    }

    public void del(String key, int db) {
        int clusterId = clusterId(key);
        if (clusterId == -1) return;
        Jedis redis = jediss.get(clusterId);
        redis.select(db);
        redis.del(key);

    }

    public void flushDB(int db) {


        for (Jedis jedis : jediss)
            jedis.flushDB();

    }

    public Set<String> keys(String s, int db) {

        Set<String> keys = new HashSet<>(1024);
        for (Jedis jedis : jediss) {
            keys.addAll(jedis.keys(s));
        }
        return keys;
    }

    @Override
    public void addNodeToCluster(String ipAddress, int port, int noOfReplicas) {
        jediss.add(new Jedis(ipAddress, port));
    }
}
