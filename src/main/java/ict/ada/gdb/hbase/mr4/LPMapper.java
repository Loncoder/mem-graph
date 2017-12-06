package ict.ada.gdb.hbase.mr4;

import ict.ada.gdb.hbase.constant.ConstantMR;
import ict.ada.gdb.hbase.util.IdParser;
import ict.ada.gdb.service.RedisInstance;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 * Created by lon on 17-2-23.
 */
public class LPMapper extends TableMapper<Text, Text> {

    // 跌段阶段只更新极点或者是上一步骤参与更新的节点.
    private static Jedis jedis = RedisInstance.getInstance().getService();

    //private double f ;
   //private static String maxSetKey = "maxSetKey**";

    private MaxSetKey setKey = null;


    //private static  int iterNum = 0;
    public LPMapper(int iterNum) {
        //f = 3;
        setKey = new MaxSetKey(iterNum-1);

    }

    //private long count = 0;

    @Override
    protected void map(ImmutableBytesWritable row, Result values, Context context) throws IOException, InterruptedException {
        ImmutableBytesWritable userKey = new ImmutableBytesWritable(row.get(), 0, Bytes.SIZEOF_INT);

        byte[] rowKey = userKey.get();

        //System.out.println("rowKey "+ StringUtils.byteToHexString(rowKey));
        String tailId = getTailId(rowKey);

        //String label = getLabel(values);

        byte[] headBytes = IdParser.getHeadId(rowKey);


        String headId = StringUtils.byteToHexString(headBytes);

        String label = getLabel(headId);

        //double factor = getFactor(headId);
        if (isOkSet(headId) && !isOkSet(tailId)) {

            context.write(new Text(tailId), new Text(label));
        }

        //System.out.println("factor : " + factor);


        /*if(factor>=f) {
            context.write(new ImmutableBytesWritable(headBytes), new Text(label + "\t" + tailId));

            if (count++ % 1000 == 0) {
                context.setStatus("Mapper count " + count);
            }
        }*/
    }


    private String getTailId(byte[] rowKey) {

        byte[] tailId = Bytes.copy(rowKey, Constant.tailIndex, Constant.IdLength);
        return StringUtils.byteToHexString(tailId);
    }

//    private String getLabel(Result values){
//
//        byte[] label = values.getValue(Bytes.toBytes(ConstantMR.columnFamily), Bytes.toBytes(ConstantMR.column));
//        return Bytes.toString(label);
//    }

//    private double getFactor(String headId){
//
//        String key = headId + pattern;
//        if(jedis.exists(key)){
//            return Double.parseDouble(jedis.get(key));
//        }
//        return 0.0;
//    }

    private String getLabel(String headId) {

        return jedis.get(headId + ConstantMR.PATTERN);
    }

    private boolean isOkSet(String headId) {

        return setKey.exist(headId);

        //return jedis.sismember(maxSetKey, headId);
    }

}
