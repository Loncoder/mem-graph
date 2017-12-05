package ict.ada.gdb.redis.consistent;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RedisProxy {


    public Map<String, String> hgetAll(String key, int db);

    public void hset(String key, String field, String value, int db);

    public boolean hexists(String key, String field, int db);

    public String hget(String key, String field, int db);

    public void lpush(String key, int db, String... value);

    public List<String> lrange(String key, long start, long end, int db);

    public void del(String key, int db);


    public void flushDB(int db);

    public Set<String> keys(String s, int db);

    public void addNodeToCluster(String ipAddress, int port, int noOfReplicas);
}
