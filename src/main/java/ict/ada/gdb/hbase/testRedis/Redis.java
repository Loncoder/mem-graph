package ict.ada.gdb.hbase.testRedis;

import ict.ada.gdb.service.RedisInstance;
import redis.clients.jedis.Jedis;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by lon on 17-5-15.
 */
public class Redis {
    //private static Jedis jedis = RedisInstance.getInstance().getService();

    private static void run(ExecutorService threadPool) {
        for (int i = 0; i < 3; i++) {
            //final int taskId = i;
            threadPool.execute(new Runnable() {
                public void run() {
                    Jedis jedis = RedisInstance.getInstance().getService();
                    for (int j = 0; j < 100000; j++) {
                        if (jedis.exists("key" + j)) {

                        }
                    }
                }
            });
        }
    }

    public static void main(String[] args) {
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        run(cachedThreadPool);
    }


}
