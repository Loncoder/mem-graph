package ict.ada.gdb.proxy;

import ict.ada.gdb.imodel.Graph;
import ict.ada.gdb.imodel.Node;
import ict.ada.gdb.imodel.Relation;
import ict.ada.gdb.model.*;
import ict.ada.gdb.query.QueryMap;
import ict.ada.gdb.query.RelQuerySpec;
import ict.ada.gdb.service.GraphService;
import ict.ada.gdb.service.MetaDataStorage;

import java.util.List;
import java.util.Map;

/**
 * Created by lon on 17-1-10.
 */
public class GraphProxy extends ProxyBase implements Graph {

    private GraphMeta meta;

    public GraphProxy(GraphService service, String graphName) {
        super(service);
        IMetaDataStorage storage = new MetaDataStorage();
        meta = storage.getGraphMetadata(graphName);
    }

    public int id() {
        return meta.getId();
    }

    public String name() {
        return meta.getName();
    }

    public List<Node> listNodes() {

        return gs.listNodes(-1);
    }

    public List<Node> listNodes(String type) {

        return gs.listNodes(getNodeType(type));
    }

    public Node getNode(String type, String id) {

        return gs.getNode(getNodeType(type), id);
    }

    public Relation getRelation(String headType, String headId, String tailType, String tailId, String type) {

        return gs.getRelation(new GDBNode(getNodeType(headType), headId), new GDBNode(getNodeType(tailType), tailId), getRelType(type));
    }


    public List<Relation> getRelations(String tailType, String relType) {

        return gs.listRelations(getNodeType(tailType), getRelType(relType));
    }

    public Node newNode(String type, String id, int value) {

        return gs.addNode(getNodeType(type), id, value);
    }

    public void deleteRelation(String headType, String headId, String tailType, String tailId, String type) {

        gs.delRelation(new GDBNode(getNodeType(headType), headId), new GDBNode(getNodeType(tailType), tailId), getRelType(type));
    }

    public void clear() {
        gs.deleteGraphMeta();
        gs.delGraph();
    }

    public Relation newRelation(String headType, String headId, String tailType, String tailId, String type, int val) {

        return gs.addRelation(new GDBNode(getNodeType(headType), headId), new GDBNode(getNodeType(tailType), tailId), getRelType(type), val);
    }

    public GraphMeta graphMeta() {
        return gs.getGraphMeta();
    }

    @Override
    public List<Node> getNodesInTwoLevel(GDBNode start) {

        return gs.twoLevelVisit(start);
    }



    public long getNodeCount() {
        return 0;
    }

    public long getRelCount() {
        return 0;
    }

    public List<GDBNode> query(int nodeType, List<QueryMap> queries, int start, int len) {
        return null;
    }

    public long queryCount(int nodeType, List<QueryMap> queries) {
        return 0;
    }

    public Map<String, Long> getNodeTypeCount() {
        return null;
    }

    public Map<String, Long> getRelationTypeCount() {
        return null;
    }

    public List<Node> shortestPath(GDBNode start , GDBNode end) {
        return gs.shortestPath(start,end);
    }

    public void delete() {
        gs.deleteGraphMeta();
        gs.delGraph();
    }

    public void close() throws Exception {

    }


}
