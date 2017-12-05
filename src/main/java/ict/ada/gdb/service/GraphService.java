package ict.ada.gdb.service;

import ict.ada.gdb.hbase.constant.ConstantMR;
import ict.ada.gdb.imodel.Attribute;
import ict.ada.gdb.imodel.Node;
import ict.ada.gdb.imodel.Relation;
import ict.ada.gdb.model.*;
import ict.ada.gdb.proxy.NodeProxy;
import ict.ada.gdb.proxy.RelationProxy;
import ict.ada.gdb.redis.consistent.RedisProxy;
import ict.ada.gdb.proxy.RedisProxyConsistent;
import org.apache.commons.collections.map.HashedMap;

import java.util.*;

/**
 * Created by lon on 17-1-10.
 */
public class GraphService {

    RedisProxy redisProxy;
    private Properties properties;

    private String graphName;

    private int graphId;

    private static String IDFLAG = "i";

    private static String ISMASTER = "m";

    private static String SLAVES = "s";

    private static String DATA = "d";

    private static String COLOR = "c";

    private static String IN = "i";

    private static String OUT = "o";

    private IMetaDataStorage metaStore = new MetaDataStorage();

    public GraphService(String graphName, Properties properties) {

        this.graphName = graphName;

        GraphMeta meta = getGraphMeta();

        if (meta != null)
            this.graphId = meta.getId();

        this.properties = properties;

        redisProxy = new RedisProxyConsistent();
        for (String host : ConstantMR.jedisHosts) {
            redisProxy.addNodeToCluster(host, 6379, 1);
        }
    }

    /**
     * 根据节点类型和节点id,获取节点参与运算的基本信息.
     **/


    public GraphMeta getGraphMeta() {

        return metaStore.getGraphMetadata(graphName);
    }

    public GraphMeta createGraphMeta() {

        this.graphId = metaStore.createGraph(graphName);
        return getGraphMeta();
    }

    public boolean deleteGraphMeta() {

        return metaStore.deleteGraph(graphName);
    }

    public Node getNode(int nType, String nodeId) {

        //    Jedis jedis = TableManager.selectNodeTable(graphId);
        //1: hgetAll
        //Map<String, String> resultMap = jedis.hgetAll(ID.makeNodeIdKey(nType, nodeId));
        int nodeDb = graphId * 3 + 2;
        Map<String, String> resultMap = redisProxy.hgetAll(ID.makeNodeIdKey(nType, nodeId), nodeDb);


        if (resultMap == null || resultMap.isEmpty())
            return null;

        int data = Integer.parseInt(resultMap.get(DATA));
        String slaves = resultMap.get(SLAVES);
        int color = Integer.parseInt(resultMap.get(COLOR));
        boolean isMaster = Boolean.parseBoolean(resultMap.get(ISMASTER));

        GDBNode node = new GDBNode(nType, nodeId);
        node.setIsMaster(isMaster);
        node.setData(data);
        node.setSlaves(slaves);
        node.setColor(color);
        return new NodeProxy(this, node);
    }

    /**
     * 在持久层增加一个节点,包括节点参与运算的值
     **/

    public Node addNode(int nType, String nodeId, int value) {
        //Jedis jedis = TableManager.selectNodeTable(graphId);
        System.out.println(ID.makeNodeIdKey(nType, nodeId) + "\t" + IDFLAG);

        // 2: hset
//        jedis.hset(ID.makeNodeIdKey(nType, nodeId), IDFLAG, nType + "_" + nodeId);
//        jedis.hset(ID.makeNodeIdKey(nType, nodeId), ISMASTER, "false");
//        jedis.hset(ID.makeNodeIdKey(nType, nodeId), SLAVES, "");
//        jedis.hset(ID.makeNodeIdKey(nType, nodeId), COLOR, "0");
//        jedis.hset(ID.makeNodeIdKey(nType, nodeId), DATA, String.valueOf(value));
        int nodeDb = graphId * 3 + 2;
        redisProxy.hset(ID.makeNodeIdKey(nType, nodeId), IDFLAG, nType + "_" + nodeId, nodeDb);
        redisProxy.hset(ID.makeNodeIdKey(nType, nodeId), ISMASTER, "false", nodeDb);
        redisProxy.hset(ID.makeNodeIdKey(nType, nodeId), SLAVES, "", nodeDb);
        redisProxy.hset(ID.makeNodeIdKey(nType, nodeId), COLOR, "0", nodeDb);
        redisProxy.hset(ID.makeNodeIdKey(nType, nodeId), DATA, String.valueOf(value), nodeDb);
        return new NodeProxy(this, new GDBNode(nType, nodeId));
    }


    public boolean existNode(int nType, String nodeId) {

        //      Jedis jedis = TableManager.selectNodeTable(graphId);

        //3: hexists
        //    return jedis.hexists(ID.makeNodeIdKey(nType, nodeId), IDFLAG);
        int nodeDb = graphId * 3 + 2;
        return redisProxy.hexists(ID.makeNodeIdKey(nType, nodeId), IDFLAG, nodeDb);
    }

    /**
     * 如果节点存在,则为节点添加属性;否则返回false
     **/
    public boolean addNodeAttr(int nType, String nodeId, String key, String value) {

        if (value == null || !existNode(nType, nodeId)) return false;

        //    Jedis jedis = TableManager.selectAttributeTable(graphId);

        // 2: hset

        int attributeDb = graphId * 3 + 1;
        redisProxy.hset(ID.makeNodeIdKey(nType, nodeId), key, value, attributeDb);
        //   jedis.hset(ID.makeNodeIdKey(nType, nodeId), key, value);

        return true;

    }

    /**
     * 获取指定节点的属性
     */
    public String getNodeAttr(int nType, String nodeId, String attrKey) {

        if (existNode(nType, nodeId)) {

//            Jedis jedis = TableManager.selectAttributeTable(graphId);
//
//    // 4: hget
//            return jedis.hget(ID.makeNodeIdKey(nType, nodeId), attrKey);
            int attributeDb = graphId * 3 + 1;
            return redisProxy.hget(ID.makeNodeIdKey(nType, nodeId), attrKey, attributeDb);

        }
        return null;
    }

    public List<Attribute> getNodeAttrs(int nType, String nodeId) {

        List<Attribute> attributes = new ArrayList<Attribute>();
        if (existNode(nType, nodeId)) {

//            Jedis jedis = TableManager.selectAttributeTable(graphId);
//
//        // 1:hgetAll
//            Map<String, String> tmpMap = jedis.hgetAll(ID.makeNodeIdKey(nType, nodeId));

            int attributeDb = graphId * 3 + 1;
            Map<String, String> tmpMap = redisProxy.hgetAll(ID.makeNodeIdKey(nType, nodeId), attributeDb);

            for (Map.Entry<String, String> entry : tmpMap.entrySet()) {
                Attribute attribute = new GDBAttribute(entry.getKey(), entry.getValue());
                attributes.add(attribute);
            }
            return attributes;
        }
        return Collections.EMPTY_LIST;
    }

    /**
     * 增加一个关系,并且增加关系中参与运算的基本值
     **/
    public Relation addRelation(GDBNode src, GDBNode des, int rType, int val) {
//        Jedis jedis = TableManager.selectRelationTable(graphId);
//
//    //  2:hset
//        jedis.hset(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), IDFLAG,
//                src.getType() + "_" + src.getId() + "_" + des.getType() + "_" + des.getId() + "_" + rType);
//        jedis.hset(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), ISMASTER, "false");
//        jedis.hset(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), SLAVES, "");
//        jedis.hset(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), COLOR, "0");
//        jedis.hset(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), DATA, String.valueOf(val));
//
//    //  5:lpush
//        jedis.lpush(ID.makeNodeIdKey(src.getType(), src.getId()), ID.makeNeighborId(des.getType(), des.getId(), rType));
//        jedis.lpush(ID.makeNodeIdKey(des.getType(), des.getId()), ID.makeNeighborId(src.getType(), src.getId(), rType));
//        return new RelationProxy(new GDBRelation(src, des, rType), this);


        //  2:hset
        int relationDbIndex = graphId * 3 + 3;
        redisProxy.hset(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), IDFLAG,
                src.getType() + "_" + src.getId() + "_" + des.getType() + "_" + des.getId() + "_" + rType, relationDbIndex);
        redisProxy.hset(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), ISMASTER, "false", relationDbIndex);
        redisProxy.hset(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), SLAVES, "", relationDbIndex);
        redisProxy.hset(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), COLOR, "0", relationDbIndex);
        redisProxy.hset(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), DATA, String.valueOf(val), relationDbIndex);

        //  5:lpush
        redisProxy.lpush(ID.makeNodeIdKey(src.getType(), src.getId()), relationDbIndex, ID.makeNeighborId(des.getType(), des.getId(), rType));
        redisProxy.lpush(ID.makeNodeIdKey(des.getType(), des.getId()), relationDbIndex, ID.makeNeighborId(src.getType(), src.getId(), rType));
        return new RelationProxy(new GDBRelation(src, des, rType), this);
    }

    public Relation getRelation(GDBNode src, GDBNode des, int rType) {
        if (!existRelation(src, des, rType)) return null;
//        Jedis jedis = TableManager.selectRelationTable(graphId);

        //1:hgetAll
        int relationDbIndex = graphId * 3 + 3;
        Map<String, String> resultMap = redisProxy.hgetAll(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), relationDbIndex);

        if (resultMap == null || resultMap.isEmpty())
            return null;
        int data = Integer.parseInt(resultMap.get(DATA));
        String slaves = resultMap.get(SLAVES);
        int color = Integer.parseInt(COLOR);
        boolean isMaster = Boolean.parseBoolean(resultMap.get(ISMASTER));

        GDBRelation relation = new GDBRelation(src, des, rType);
        relation.setIsMaster(isMaster);
        relation.setData(data);
        relation.setSlaves(slaves);
        relation.setColor(color);
        return new RelationProxy(relation, this);
    }

    /**
     * 判断指定首位节点以及关系类型,是否存在关系
     */
    public boolean existRelation(GDBNode src, GDBNode des, int rType) {
        // Jedis jedis = TableManager.selectRelationTable(graphId);
        int relationDbIndex = graphId * 3 + 3;
        return redisProxy.hexists(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), IDFLAG, relationDbIndex);
    }

    /**
     * 为指定的关系添加属性
     **/
    public boolean addRelationAttr(GDBNode src, GDBNode des, int rType, String key, String value) {

        if (value == null || !existRelation(src, des, rType)) return false;
        // Jedis jedis = TableManager.selectAttributeTable(graphId);
        int attributeDb = graphId * 3 + 1;
        redisProxy.hset(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), key, value.toString(), attributeDb);

        return true;
    }

    /**
     * 获取指定节点的属性
     **/
    public String getRelationAttr(GDBNode src, GDBNode des, int rType, String attrKey) {

        if (existRelation(src, des, rType)) {
            //Jedis jedis = TableManager.selectAttributeTable(graphId);
            int attributeDb = graphId * 3 + 1;
            return redisProxy.hget(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), attrKey, attributeDb);

        }
        return null;
    }

    public List<Attribute> getRelationAttrs(GDBNode src, GDBNode des, int rType) {

        List<Attribute> attributes = new ArrayList<Attribute>();

        if (existRelation(src, des, rType)) {

            //Jedis jedis = TableManager.selectAttributeTable(graphId);
            int attributeDb = graphId * 3 + 1;
            Map<String, String> tmpMap = redisProxy.hgetAll(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), attributeDb);
            for (Map.Entry<String, String> entry : tmpMap.entrySet()) {
                Attribute attribute = new GDBAttribute(entry.getKey(), entry.getValue());
                attributes.add(attribute);
            }
            return attributes;
        }


        return Collections.EMPTY_LIST;
    }

    /**
     * 获取指定节点的入节点
     * 如果rtype==null,返回所有关系类型的节点
     **/
    public Collection<Node> neighbors(GDBNode src, int tailType, int rType) {

        List<Node> nodes = new ArrayList<Node>();
        if (existNode(src.getType(), src.getId())) {
            //Jedis jedis = TableManager.selectNodeTable(graphId);

            //6: lrange
            int nodeDb = graphId * 3 + 2;
            List<String> neighbors = redisProxy.lrange(ID.makeNodeIdKey(src.getType(), src.getId()), 0, -1, nodeDb);
            for (String neighborId : neighbors) {
                String[] array = neighborId.split(ID.ConStr);
                int tT = Integer.parseInt(array[0]);
                int rT = Integer.parseInt(array[1]);
                if (tailType != -1 && tailType != tT) continue;
                if (rType != -1 && rT != rType) continue;
                nodes.add(new NodeProxy(this, new GDBNode(tailType, array[1])));
            }
            return nodes;
        }
        return Collections.emptyList();
    }


    /*************
     * Update
     ****************/

    public Node updateNodeValue(GDBNode src, int newValue) {

        if (existNode(src.getType(), src.getId())) {
            //Jedis jedis = TableManager.selectNodeTable(graphId);
            int nodeDb = graphId * 3 + 2;
            redisProxy.hset(ID.makeNodeIdKey(src.getType(), src.getId()), DATA, String.valueOf(newValue), nodeDb);
            src.setData(newValue);
            return new NodeProxy(this, src);
        }
        return null;

    }

    public Relation updateRelationValue(GDBNode src, GDBNode des, int rType, int newValue) {
        if (existRelation(src, des, rType)) {
            //Jedis jedis = TableManager.selectRelationTable(graphId);
            int relationDbIndex = graphId * 3 + 3;
            redisProxy.hset(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), DATA, String.valueOf(newValue), relationDbIndex);
            GDBRelation relation = new GDBRelation(src, des, rType);
            relation.setData(newValue);
        }
        return null;
    }

    public boolean updateNodeAttribute(int nType, String nodeId, Attribute attr) {

        if (attr.value() == null || !existNode(nType, nodeId)) return false;
        int attributeDb = graphId * 3 + 1;
        //Jedis jedis = TableManager.selectAttributeTable(graphId);
        redisProxy.hset(ID.makeNodeIdKey(nType, nodeId), attr.key(), attr.value(), attributeDb);

        return true;
    }

    public boolean updateRelationAttribute(GDBNode src, GDBNode des, int rType, Attribute attr) {

        if (attr.value() == null || !existRelation(src, des, rType)) return false;
        //Jedis jedis = TableManager.selectAttributeTable(graphId);
        int attributeDb = graphId * 3 + 1;
        redisProxy.hset(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), attr.key(), attr.value(), attributeDb);

        return true;
    }


    /*****************
     * DEl
     ***********************/
    public boolean delNode(int nType, String nodeId) {

        //7: del
        if (existNode(nType, nodeId)) {
            //Jedis jedis = TableManager.selectAttributeTable(graphId);
            int attributeDb = graphId * 3 + 1;
            redisProxy.del(ID.makeNodeIdKey(nType, nodeId), attributeDb);
            //jedis = TableManager.selectNodeTable(graphId);
            int nodeDb = graphId * 3 + 2;
            redisProxy.del(ID.makeNodeIdKey(nType, nodeId), nodeDb);
            // 还没有删除关系
            return true;
        }

        return false;
    }

    public boolean delNodeAttr(int nType, String nodeId, String attrKey) {

        if (existNode(nType, nodeId)) {
            //Jedis jedis = TableManager.selectAttributeTable(graphId);
            int attributeDb = graphId * 3 + 1;
            redisProxy.del(ID.makeNodeIdKey(nType, nodeId), attributeDb);
            return true;
        }

        return false;
    }

    public boolean delRelation(GDBNode src, GDBNode des, int rType) {

        if (existRelation(src, des, rType)) {
            int attributeDb = graphId * 3 + 1;
            //Jedis jedis = TableManager.selectAttributeTable(graphId);
            redisProxy.del(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), attributeDb);
            //jedis = TableManager.selectRelationTable(graphId);
            int relationDbIndex = graphId * 3 + 3;
            redisProxy.del(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), relationDbIndex);

            return true;
        }
        return false;
    }

    public boolean delRelationAttr(GDBNode src, GDBNode des, int rType, String attrKey) {

        if (existRelation(src, des, rType)) {
            //Jedis jedis = TableManager.selectAttributeTable(graphId);
            int attributeDb = graphId * 3 + 1;
            redisProxy.del(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), attributeDb);
            return true;
        }
        return false;
    }

    public boolean delGraph() {

        if (getGraphMeta() == null) return false;

        //9 flushDB
        //Jedis jedis = TableManager.selectAttributeTable(graphId);
        int graphMetaIndex = 0;
        redisProxy.flushDB(graphMetaIndex);
        //jedis = TableManager.selectNodeTable(graphId);
        //jedis.flushDB();
        //jedis = TableManager.selectRelationTable(graphId);
        //jedis.flushDB();
        return true;
    }

    public List<Node> listNodes(int nType) {

        //Jedis jedis = TableManager.selectNodeTable(graphId);

        //10 keys
        int nodeDb = graphId * 3 + 2;
        Set<String> keys = redisProxy.keys("*", nodeDb);
        List<Node> nodes = new LinkedList<Node>();
        for (String key : keys) {
            String[] array = key.split(ID.ConStr);

            int nT = Integer.parseInt(array[0]);
            if (nType != -1 && nT != nType) continue;

            Map<String, String> resultMap = redisProxy.hgetAll(ID.makeNodeIdKey(nT, array[1]), nodeDb);

            int data = Integer.parseInt(resultMap.get(DATA));
            String slaves = resultMap.get(SLAVES);
            int color = Integer.parseInt(COLOR);
            boolean isMaster = Boolean.parseBoolean(resultMap.get(ISMASTER));

            GDBNode node = new GDBNode(nT, array[1]);
            node.setIsMaster(isMaster);
            node.setData(data);
            node.setSlaves(slaves);
            node.setColor(color);
            nodes.add(new NodeProxy(this, node));
        }
        return nodes;
    }

    public List<Relation> listRelations(int tType, int rType) {
        //Jedis jedis = TableManager.selectRelationTable(graphId);
        int relationDbIndex = graphId * 3 + 3;
        Set<String> keys = redisProxy.keys("*", relationDbIndex);
        List<Relation> relations = new LinkedList<Relation>();
        for (String key : keys) {
            String[] array = key.split(ID.ConStr);
            GDBNode src = new GDBNode(Integer.parseInt(array[0]), array[1]);
            GDBNode des = new GDBNode(Integer.parseInt(array[2]), array[3]);
            int rT = Integer.parseInt(array[4]);
            if ((tType != -1 && tType != des.getType()) || (rType != -1 && rType != rT)) continue;

            Map<String, String> resultMap = redisProxy.hgetAll(ID.makeRelationIdKey(src.getType(), src.getId(), des.getType(), des.getId(), rType), relationDbIndex);

            if (resultMap == null || resultMap.isEmpty())
                return null;
            int data = Integer.parseInt(resultMap.get(DATA));
            String slaves = resultMap.get(SLAVES);
            int color = Integer.parseInt(COLOR);
            boolean isMaster = Boolean.parseBoolean(resultMap.get(ISMASTER));

            GDBRelation relation = new GDBRelation(src, des, rType);
            relation.setIsMaster(isMaster);
            relation.setData(data);
            relation.setSlaves(slaves);
            relation.setColor(color);
            relations.add(new RelationProxy(relation, this));
        }
        return relations;
    }

    public List<Node> twoLevelVisit(GDBNode start) {

        List<Node> nodes = new ArrayList<>();
        Collection<Node> firstLevel = neighbors(start, -1, -1);
        int count = 0;
        int max = 100;
        for (Node node : firstLevel) {
            if (count++ > max) break;
            nodes.addAll(node.neighbors());
        }
        return nodes;
    }

    public List<Node> shortestPath(GDBNode start, GDBNode end) {
        List<Node> paths = new ArrayList<>();
        Queue<GDBNode> queue = new LinkedList<>();
        queue.add(start);
        int curDepth = 1;
        int depth = 3;
        Set<String> nodeSet = new HashSet<>(1024);
        nodeSet.add(start.getType() + "_" + start.getId());
        Map<GDBNode, GDBNode> parents = new HashedMap();
        parents.put(start, start);
        while (!queue.isEmpty() && curDepth < depth) {
            if (curDepth > 4) break;
            int len = queue.size();
            for (int i = 0; i < len; i++) {
                GDBNode node = queue.peek();
                queue.poll();
                Collection<Node> outNodes = neighbors(node, -1, -1);
                for (Node outNode : outNodes) {
                    String outNodeId = outNode.type() + "_" + outNode.type();
                    if (!nodeSet.add(outNodeId)) continue;
                    parents.put(outNode.gdbNode(), node);
                    if (outNode.type().equals(end.getType()) && outNode.id().equals(end.getId())) {

                        paths.add(new NodeProxy(this, end));
                        GDBNode parent = end;
                        while (!parents.equals(start)) {
                            parent = parents.get(parent);
                            paths.add(new NodeProxy(this, parent));
                        }
                        break;
                    }
                    queue.add(outNode.gdbNode());
                }
            }
            curDepth++;
        }
        return paths;
    }
}

