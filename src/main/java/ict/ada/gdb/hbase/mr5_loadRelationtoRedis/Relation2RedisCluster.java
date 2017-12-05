package ict.ada.gdb.hbase.mr5_loadRelationtoRedis;

import ict.ada.gdb.hbase.constant.ConstantMR;
import ict.ada.gdb.hbase.graph.GraphId;
import ict.ada.gdb.hbase.mr2_Factor.Main;
import ict.ada.gdb.hbase.mr4.Constant;
import ict.ada.gdb.hbase.util.IdParser;
import ict.ada.gdb.service.RedisInstance;
import ict.ada.gdb.util.ConfUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class Relation2RedisCluster extends Configured implements Tool {

    //byte [] value1 = new byte[0];
    private static Jedis jedis = RedisInstance.getInstance().getService();

    static List<Jedis> jediss = new ArrayList<>();

    private Random random = new Random();

    static {
        jedis.select(15);
        for (String host : ConstantMR.jedisHosts) {
            jediss.add(new Jedis(host, 6379, 1));
        }
    }

    public class LoadMap extends TableMapper<ImmutableBytesWritable, Text> {
        protected void map(ImmutableBytesWritable rowKey, Result value, Context context) {


            byte[] bHeadId = IdParser.getHeadId(rowKey.get());

            byte[] bTailId = IdParser.getTailId(rowKey.get());

            int clusterIndex ;

            if(jedis.exists(StringUtils.byteToHexString(bHeadId)+ConstantMR.PATTERN))
            {
                String label = jedis.get(StringUtils.byteToHexString(bHeadId)+ConstantMR.PATTERN);

                clusterIndex =  label.hashCode()% jediss.size();
            }else{

                clusterIndex = StringUtils.byteToHexString(bHeadId).hashCode()% jediss.size();
            }

            byte[] id = Bytes.add(bHeadId, bTailId);

            jediss.get(clusterIndex % jediss.size()).incr(StringUtils.byteToHexString(id));
        }
    }


    public int run(String[] strings) throws Exception {
        if (strings.length != 1) {
            System.out.println("Usage : con-path");
        }
        Configuration conf = getConf();
        String path = strings[0];

        Configuration conf_e = ConfUtil.getConf(path);
        int graphId = conf_e.getInt("graphId", 0);

        String linkGraphName = GraphId.getRelationTable(graphId);

        conf.set(Constant.ZK_HOST, conf_e.get(Constant.ZK_HOST), "10.100.1.35");
        conf.set(Constant.ZK_PORT, conf_e.get(Constant.ZK_PORT), "2181");


        Job job = new Job(conf, "load-relation");
        job.setJarByClass(Main.class);
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        scan.addFamily(Bytes.toBytes(Constant.columnFamily));


        TableMapReduceUtil.initTableMapperJob(linkGraphName, scan, LoadMap.class, ImmutableBytesWritable.class,
                Text.class, job);
        //TableMapReduceUtil.initTableReducerJob(linkGraphName, ImFactorReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //使用ToolRunner的run方法对自定义的类型进行处理
        ToolRunner.run(conf, new Main(), args);
    }
}
