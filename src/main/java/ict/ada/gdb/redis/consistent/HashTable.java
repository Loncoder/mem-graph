package ict.ada.gdb.redis.consistent;

import redis.clients.jedis.Jedis;

/**
 * Created by lon on 17-4-16.
 */
public class HashTable {
    static Jedis jedis = new Jedis("10.100.1.35", 6379);

    {
        jedis.select(15);
    }

    public static int getColor(String key) {
        if (jedis.exists(key)) return Integer.parseInt(jedis.get(key));
        return 0;
    }
}
