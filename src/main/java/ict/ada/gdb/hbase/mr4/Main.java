package ict.ada.gdb.hbase.mr4;

import ict.ada.gdb.hbase.graph.GraphId;
import ict.ada.gdb.service.RedisInstance;
import ict.ada.gdb.util.ConfUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import redis.clients.jedis.Jedis;


/**
 * Created by lon on 17-2-23.
 */
public class Main extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //使用ToolRunner的run方法对自定义的类型进行处理
        ToolRunner.run(conf, new Main(), args);
    }

    private static Jedis jedis = RedisInstance.getInstance().getService();
    private static String maxSetKey = "maxSetKey**";//过大时候 需要分key 存储，暂时放在一个key中存储
    //已经打好标签的节点； 又或者再选择一张表进行存储

    public int run(String[] strings) throws Exception {
        if (strings.length != 1) {
            System.out.println("Usage : con-path");
        }


        Configuration conf = getConf();
        String path = strings[0];

        Configuration conf_e = ConfUtil.getConf(path);
        int graphId = conf_e.getInt("graphId", 0);

        int iterNum = conf_e.getInt("iter", 1);

        maxSetKey = maxSetKey + (iterNum == 1 ? "" : iterNum - 1);
        int seed = jedis.hkeys(maxSetKey).size();
        if (seed < 100) {
            System.out.println("iter seed size is too small");
            return -1;
        }


        String linkGraphName = GraphId.getRelationTable(graphId);

        conf.set(Constant.ZK_HOST, conf_e.get(Constant.ZK_HOST), "127.0.0.1");
        conf.set(Constant.ZK_PORT, conf_e.get(Constant.ZK_PORT), "2181");


        Job job = new Job(conf, "Hbase-LP");
        job.setJarByClass(Main.class);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Constant.columnFamily));

        TableMapReduceUtil.initTableMapperJob(linkGraphName, scan, LPMapper.class, ImmutableBytesWritable.class,
                Text.class, job);
        TableMapReduceUtil.initTableReducerJob(linkGraphName, LPReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}
