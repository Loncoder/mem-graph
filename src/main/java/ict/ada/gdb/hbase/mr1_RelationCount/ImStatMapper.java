package ict.ada.gdb.hbase.mr1_RelationCount;

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
public class ImStatMapper extends TableMapper<ImmutableBytesWritable, Text> {

    private long count = 0;

    private static Jedis jedis = RedisInstance.getInstance().getService();

    @Override
    protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

        jedis.select(15);
        byte[] rowKey = row.get();

        System.out.println("rowKey " + StringUtils.byteToHexString(rowKey));

        byte[] bHeadId = IdParser.getHeadId(rowKey);

        byte[] bTailId = IdParser.getTailId(rowKey);

        jedis.incr(StringUtils.byteToHexString(bHeadId));

        jedis.incr(StringUtils.byteToHexString(bTailId));

        //jedis.
    }


}
