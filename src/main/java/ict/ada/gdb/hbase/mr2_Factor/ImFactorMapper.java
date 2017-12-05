package ict.ada.gdb.hbase.mr2_Factor;

import ict.ada.gdb.hbase.util.IdParser;
import ict.ada.gdb.service.RedisInstance;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 * Created by lon on 17-3-13.
 */
public class ImFactorMapper extends TableMapper<ImmutableBytesWritable, Text> {

    // 在此图中的weight表示两个节点之间发生关系的次数.
    // headNum= headCount*weight; 因为在第一步redis中是对所有的rowkey进行遍历叠加.
    private long count = 0;

    private static Jedis jedis = RedisInstance.getInstance().getService();

    @Override
    protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

        byte[] rowKey = row.get();


        byte[] bHeadId = IdParser.getHeadId(rowKey);

        byte[] bTailId = IdParser.getTailId(rowKey);

        String headId = StringUtils.byteToHexString(bTailId);

        String tailId = StringUtils.byteToHexString(bTailId);

        int headNum = 0;

        if (jedis.exists(headId)) {

            headNum = (int) Double.parseDouble(jedis.get(headId));

        }

        int tailNum = 0;

        if (jedis.exists(tailId)) {

            tailNum = (int) Double.parseDouble(jedis.get(tailId));

        }

        context.write(new ImmutableBytesWritable(bHeadId), new Text(headNum + "\t" + tailNum));

        if (count++ % 1000 == 0) {

            context.setStatus("Mapper count " + count);

        }

    }


}
