package ict.ada.gdb.hbase.mr4;

import ict.ada.gdb.hbase.constant.ConstantMR;
import ict.ada.gdb.service.RedisInstance;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lon on 17-2-23.
 */
public class LPReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
    static int iterNum = 0;

    private static Jedis jedis = RedisInstance.getInstance().getService();


    private static String maxSetKey = "maxSetKey**";

    //private static  int iterNum = 0;
    public LPReducer(int iterNum) {
        //f = 3;
        maxSetKey = maxSetKey + (iterNum);
    }


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        if (values == null) return;

        List<String> neighbors = new ArrayList<String>();

        Map<String, Integer> labelRanks = new HashMap<String, Integer>();

        for (Text value : values){

            String v = value.toString();

            if(labelRanks.containsKey(value)){

                labelRanks.put(v,labelRanks.get(v)+1);
            }else
                labelRanks.put(v,1);
        }
        String maxLabel = "";
        Integer maxValue = 0;
        for (Map.Entry<String,Integer> entry : labelRanks.entrySet()){

            if(entry.getValue()>maxValue){

                maxValue=entry.getValue();

                maxLabel = entry.getKey();
            }
        }

        for (Text value : values){

            String v = value.toString();

            jedis.set(v+ ConstantMR.PATTERN, maxLabel);

            jedis.sadd(maxSetKey,v);
        }



//        System.out.println(labelRanks);
//
//        for(String out : neighbors){
//
//            jedis.sadd(maxSetKey,out);
//
//            byte [] headId = key.get();
//
//            byte[] relationId = IdMaker.internalId(headId, out);
//
//            System.out.println(StringUtils.byteToHexString(relationId));
//
//            Put put = new Put(relationId);
//
//            put.add(Bytes.toBytes(Constant.columnFamily), Bytes.toBytes(Constant.column), Bytes.toBytes(label));
//
//            context.write(key, put);
//        }
    }
}
