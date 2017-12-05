package ict.ada.gdb.hbase.mr1_RelationCount;

import ict.ada.gdb.hbase.graph.GraphId;
import ict.ada.gdb.hbase.mr4.Constant;
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


/**
 * Created by lon on 17-2-23.
 */
public class Main extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //使用ToolRunner的run方法对自定义的类型进行处理
        ToolRunner.run(conf, new Main(), args);
    }

    public int run(String[] strings) throws Exception {
        if (strings.length != 1) {
            System.out.println("Usage : con-path");
        }
        String path = strings[0];
        Configuration conf = getConf();
        Configuration conf_e = ConfUtil.getConf(path);
        int graphId = conf_e.getInt("graphId", 0);

        String linkGraphName = GraphId.getRelationTable(graphId);


        conf.set(Constant.ZK_HOST, conf_e.get(Constant.ZK_HOST), "10.100.1.35");
        conf.set(Constant.ZK_PORT, conf_e.get(Constant.ZK_PORT), "2181");

        Job job = new Job(conf, "Hbase-Factor");
        job.setJarByClass(Main.class);
        job.setNumReduceTasks(0);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Constant.columnFamily));

        TableMapReduceUtil.initTableMapperJob(linkGraphName, scan, ImStatMapper.class, ImmutableBytesWritable.class,
                Text.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}
