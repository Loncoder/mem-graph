package ict.ada.gdb.redis.consistent.hash;

import ict.ada.gdb.redis.consistent.resources.RedisData;
import ict.ada.gdb.redis.consistent.resources.SuperNode;
import redis.clients.util.MurmurHash;

/**
 * Created by lon on 17-2-10.
 */
public class Hasher {
    private static MurmurHash murmurHash = new MurmurHash();

    public static Long getHash(Object hashable) {
        if (hashable instanceof SuperNode) {
            SuperNode superNode = (SuperNode) hashable;
            String hashString = "http://" + superNode.getIpAddress() + ":" + superNode.getPort() + "/";
            return murmurHash.hash(hashString);
        } else if (hashable instanceof RedisData) {
            //return murmurHash.hash(((RedisData)hashable).getKey());
            return murmurHash.hash(String.valueOf(((RedisData) hashable).getColor()));
        } else {
            return murmurHash.hash(hashable.toString());
        }
    }

    public static Long getReplicaHash(SuperNode superNode, int replicaID) {
        String hashString = "http://" + superNode.getIpAddress() + ":" + superNode.getPort() + "/ -- " + replicaID;
        return murmurHash.hash(hashString);
    }

    public static Integer[] getNumArray(Integer input) {
        int len = Integer.toString(input).length();
        Integer[] iarray = new Integer[len];
        for (int index = 0; index < len; index++) {
            iarray[index] = input % 10;
            input /= 10;
        }
        return iarray;
    }
}
