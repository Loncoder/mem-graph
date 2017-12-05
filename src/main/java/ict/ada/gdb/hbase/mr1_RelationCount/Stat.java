package ict.ada.gdb.hbase.mr1_RelationCount;

import ict.ada.gdb.hbase.graph.GraphId;
import ict.ada.gdb.hbase.mr4.Constant;
import ict.ada.gdb.hbase.util.IdParser;
import ict.ada.gdb.service.RedisInstance;
import ict.ada.gdb.util.ConfUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 * Created by lon on 17-3-14.
 */
public class Stat {

    static long num = 0;
    private static Jedis jedis = RedisInstance.getInstance().getService();

    static {
        jedis.select(15);
    }

    public static void main(String[] args) throws IOException {
        for (String arg : args) {
            System.out.println(arg);
        }
        if (args.length != 1) {
            System.out.println("Usage : con-path");
            return;
        }

        String conPath = args[0];
        Configuration conf = ConfUtil.getConf(conPath);
        int graphId = conf.getInt("graphId", 0);
        String graphName = GraphId.getRelationTable(graphId);

        HBaseConfiguration hbaseConf = new HBaseConfiguration();
        hbaseConf.set(Constant.ZK_HOST, conf.get(Constant.ZK_HOST), "127.0.0.1");
        hbaseConf.set(Constant.ZK_PORT, conf.get(Constant.ZK_PORT), "2181");

        Scan scan = new Scan();
        scan.setFilter(new KeyOnlyFilter());
        ResultScanner rs;
        HTable table = new HTable(hbaseConf, Bytes.toBytes(graphName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                byte[] rowKey = r.getRow();

                byte[] bHeadId = IdParser.getHeadId(rowKey);

                String sHeadId = StringUtils.byteToHexString(bHeadId);

                jedis.incr(sHeadId);

                if (num++ % 1000 == 0) {
                    System.out.println("now on num " + num);
                }

            }
        } catch (IOException e) {

        }

    }
}
