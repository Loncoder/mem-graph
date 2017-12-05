package ict.ada.gdb.service;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by lon on 17-1-10.
 */
public class RedisInstance {
    //private static Jedis jedis = null;
    private static JedisPool pool = null;

    static {
        pool = new JedisPool(new JedisPoolConfig(), "10.100.1.35");

    }

    private static RedisInstance ourInstance = new RedisInstance();

    public static RedisInstance getInstance() {
        return ourInstance;
    }

    public static Jedis getService() {
        return pool.getResource();
    }
}
