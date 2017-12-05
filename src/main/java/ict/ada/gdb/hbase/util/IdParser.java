package ict.ada.gdb.hbase.util;

import ict.ada.gdb.hbase.mr4.Constant;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by lon on 17-3-13.
 */
public class IdParser {
    public static byte[] getHeadId(byte[] rowKey) {
        return Bytes.copy(rowKey, Constant.headIndex, Constant.IdLength);
    }

    public static byte[] getTailId(byte[] rowKey) {

        byte[] tailId = Bytes.copy(rowKey, Constant.tailIndex, Constant.IdLength);
        return tailId;
    }
}
