package ict.ada.gdb.hbase.constant;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lon on 17-5-15.
 */
public class ConstantMR {
    public static final String MAXSETKEY = "maxSetKey**";

    public static final String PATTERN = "-";
    public static List<String> jedisHosts = new ArrayList<>();
    //public static List<Jedis> jediss = new ArrayList<>();

    static {
        jedisHosts.add("10.100.1.36");
        jedisHosts.add("10.100.1.37");
        jedisHosts.add("10.100.1.38");
        jedisHosts.add("10.100.1.39");
        jedisHosts.add("10.100.1.40");
        jedisHosts.add("10.100.1.41");
        jedisHosts.add("10.100.1.42");
        jedisHosts.add("10.100.1.43");
        jedisHosts.add("10.100.1.44");
    }
}
