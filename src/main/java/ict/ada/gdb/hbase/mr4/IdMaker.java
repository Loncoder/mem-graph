package ict.ada.gdb.hbase.mr4;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

/**
 * Created by lon on 17-2-28.
 */
public class IdMaker {
    public static byte[] internalId(byte[] headId, String tailId) {


        byte[] tailBytesId = StringUtils.hexStringToByte(tailId);

        byte dirBytes = (byte) Constant.dirIndex;
        byte[] linkTypeBytes = Bytes.toBytes(Constant.dirIndex);

        byte saltKeyBytes = relationSaltHash(headId, tailBytesId, Constant.headSaltBitsSize);

        byte[] headByte = Bytes.add(new byte[]{saltKeyBytes}, headId);
        byte[] tailByte = Bytes.add(new byte[]{dirBytes}, tailBytesId, linkTypeBytes);
        return Bytes.add(headByte, tailByte);
    }

    private static byte nodeSaltHash(byte[] bytes) {
        // This hash function is from Arrays.hashCode(byte[])
        int result = hash(bytes);
        return (byte) ((result % 256 + 256) % 256);
    }

    private static byte headSaltKey(byte[] headMd5, int headSaltBitsSize) {
        byte saltKey = nodeSaltHash(headMd5);
        return (byte) (saltKey >> (8 - headSaltBitsSize) << (8 - headSaltBitsSize));
    }

    private static int hash(byte[] bytes) {
        if (bytes == null) return 0;
        int result = 1;
        for (byte element : bytes) {
            result = 31 * result + element;
        }
        return result;
    }

    private static byte relationSaltHash(byte[] headMd5, byte[] tailMd5, int headSaltBitsSize) {
        headMd5 = Bytes.copy(headMd5, 1, 16);
        tailMd5 = Bytes.copy(tailMd5, 1, 16);
        byte b = headSaltKey(headMd5, headSaltBitsSize);
        byte t = nodeSaltHash(tailMd5);// 好大的一个bug,在下面t%4时候,可能出现负数
        return (byte) (b + t % (1 << (8 - headSaltBitsSize)));
    }
}
