package ict.ada.gdb.hbase.mr4;


import ict.ada.gdb.service.RedisInstance;
import redis.clients.jedis.Jedis;

public class MaxSetKey {

    private String prefix = "";
    static String value = "";
    private static Jedis jedis = RedisInstance.getInstance().getService();

    static {

        jedis.select(14);


    }

    public MaxSetKey(int iter_num) {

        prefix = Integer.toHexString(iter_num);
    }


    public boolean exist(String key) {

        return jedis.exists(key + prefix);
    }

    public void addKey(String key) {

        jedis.set(key + prefix, "");
    }
}
