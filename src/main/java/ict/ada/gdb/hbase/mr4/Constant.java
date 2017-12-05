package ict.ada.gdb.hbase.mr4;

/**
 * Created by lon on 17-2-23.
 */
public class Constant {
    public static String labelTag = "1\t";

    public static String linkTag = "2\t";

    public static String columnFamily = "lcf";

    public static String column = "lc";

    public static String fcolumn = "fc";

    public static String ZK_HOST = "hbase.zookeeper.quorum";

    public static String FS = "fs.defaultFS";

    public static String YARN = "yarn.resourcemanager.hostname";

    public static String ZK_PORT = "hbase.zookeeper.property.clientPort";

    public static int headIndex = 1;

    public static int IdLength = 16 + 1;

    public static int tailIndex = 2 + IdLength;

    public static int linkTypeIndex = 0;// 为了方便计算将所有的两个节点之间值存在一种关系类型,内部ID=0

    public static int dirIndex = 0x01; // 为了计算方便将计算节点之间的方向定义为一种,方向为out,内部编码为0x01;


    public static int headSaltBitsSize = 6;

    public static double F = 0.90;

}
