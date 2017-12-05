package ict.ada.gdb.hbase.mr2_Factor;

import ict.ada.gdb.service.RedisInstance;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 * Created by lon on 17-3-13.
 */
public class ImFactorReducer extends TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {
    private static Jedis jedis = RedisInstance.getInstance().getService();
    //0.9 是预估算出来的，论文公式


    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        if (values == null || !values.iterator().hasNext()) return;

        //List<byte [] > ids = new ArrayList<byte[]>();

        int sum = 0;

        int headNum = 0;

        for (Text text : values) {

            String v = text.toString();

            String[] array = v.split("\t");

            int tailNum = Integer.parseInt(array[1]);

            sum += tailNum;

            headNum = (int) Double.parseDouble(array[0]);

        }

        double factor = 1 + headNum + 0.9 * sum;

        String bHeadId = StringUtils.byteToHexString(key.get());
        jedis.incr(String.valueOf((int) factor));

        jedis.incrByFloat(bHeadId, factor - headNum);


 /*       for(byte[] row  : ids){

            Put put = new Put(row);

            put.add(Bytes.toBytes(ConstantMR.columnFamily), Bytes.toBytes(ConstantMR.fcolumn), Bytes.toBytes(factor));

            context.write(new ImmutableBytesWritable(row),put);
        }
*/

    }
}
