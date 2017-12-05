package ict.ada.gdb.hbase.graph;

/**
 * Created by lon on 17-2-24.
 */
public class GraphId {
    public static String getNodeTable(int graphId) {
        return "gdb-" + getInternalGraphId(graphId) + "-node";
    }

    public static String getRelationTable(int graphId) {
        return "gdb-" + getInternalGraphId(graphId) + "-link";
    }

    private static String getInternalGraphId(int graphId) {
        return String.format("%04x", graphId);
    }

}
