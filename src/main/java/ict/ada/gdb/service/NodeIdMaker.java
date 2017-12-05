package ict.ada.gdb.service;
import ict.ada.gdb.util.UByte;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.util.StringUtils;

public class NodeIdMaker {
    private int nodeType;
    private String nodeId;
    public static int hash(byte [] bytes){
        if (bytes == null) return 0;
        int result = 1;
        for (byte element : bytes) {
            result = 31 * result + element;
        }
        return result;
    }
    public NodeIdMaker(int nodeType, String nodeId) {
        this.nodeType = nodeType;
        this.nodeId = MD5Hash.getMD5AsHex(Bytes.toBytes(nodeId));
    }

    public static byte nodeSaltHash(byte[] bytes) {
        // This hash function is from Arrays.hashCode(byte[])
        int result = hash(bytes);
        return (byte) ((result%256 + 256) % 256);
    }

    public byte[] makeInternalId() {
        byte[] md5Id = StringUtils.hexStringToByte(nodeId);

        byte[] nt = new byte[]{UByte.get(nodeType).byteValue()};
        byte[] saltKey = new byte[]{nodeSaltHash(md5Id)};
        byte[] internalId = Bytes.add(saltKey, nt, md5Id);

        return internalId;
    }
}
