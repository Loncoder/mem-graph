package ict.ada.gdb.redis.consistent.server;

import ict.ada.gdb.redis.consistent.HashTable;
import ict.ada.gdb.redis.consistent.hash.Hasher;
import ict.ada.gdb.redis.consistent.json.KeyDetailsJSON;
import ict.ada.gdb.redis.consistent.resources.Node;
import ict.ada.gdb.redis.consistent.resources.RedisData;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * Created by lon on 17-2-10.
 */

public class RedisServer {
    private SortedMap<Long, Node> nodeMap;
    private List<Long> nodeHashSet;
    private SortedMap<Long, Node> distinctNodes;

    public RedisServer() {
        nodeMap = new TreeMap<Long, Node>();
        nodeHashSet = new ArrayList<Long>();
        distinctNodes = new TreeMap<Long, Node>();
    }

    public List<Long> getNodeHashSet() {
        return nodeHashSet;
    }

    public void setNodeHashSet(List<Long> nodeHashSet) {
        this.nodeHashSet = nodeHashSet;
    }

    public SortedMap<Long, Node> getDistinctNodes() {
        return distinctNodes;
    }

    public void setDistinctNodes(SortedMap<Long, Node> distinctNodes) {
        this.distinctNodes = distinctNodes;
    }

    public List<Long> getDataSet() {
        return nodeHashSet;
    }

    public void setDataSet(List<Long> dataSet) {
        this.nodeHashSet = dataSet;
    }

    public SortedMap<Long, Node> getNodeMap() {
        return nodeMap;
    }

    public void setNodeMap(SortedMap<Long, Node> nodeMap) {
        this.nodeMap = nodeMap;
    }

    public List<KeyDetailsJSON> addNodeToCluster(String ipAddress, int port, int noOfReplicas) throws Exception {
        Node node = new Node(ipAddress, port);
        Node affectedNode = null;
        Long lastNodeHash = null;
        Long replicaHash = null;
        List<KeyDetailsJSON> migratedKeys = null;
        int replicaIndex = -1;

        if (checkIfNodeIsUp(node)) {
            migratedKeys = new ArrayList<KeyDetailsJSON>();
/**
 * Added line to flush the db before data insertion
 */
            node.getJedis().flushAll();
            nodeHashSet.add(Hasher.getHash(node));
            distinctNodes.put(Hasher.getHash(node), node);
            Collections.sort(nodeHashSet);
//If it is the first node, add it immediately and exit the method
            if (nodeMap.size() == 0) {
                nodeMap.put(Hasher.getHash(node), node);
                if (noOfReplicas > 0) {
                    for (int i = 1; i <= noOfReplicas; i++) {
                        nodeHashSet.add(Hasher.getReplicaHash(node, i));
                        nodeMap.put(Hasher.getReplicaHash(node, i), node);
                    }
                }
                return migratedKeys;
            }
//Size of cluster is greater than 1
//TODO Migrate the adjacent nodes data
//Find the index at which the node is placed
            int nodeIndex = nodeHashSet.indexOf(Hasher.getHash(node));
//Get the node immediately after it
            if (nodeIndex == nodeHashSet.size() - 1) {
//This is the last node in the map. The affected node is the one at 0th position.
                affectedNode = nodeMap.get(nodeHashSet.get(0));
//Checking for all data pairs whose hash values are greater than last node's hash and are assigned to node 0
                lastNodeHash = nodeHashSet.get(nodeHashSet.size() - 1);
            } else {
                affectedNode = nodeMap.get(nodeHashSet.get(nodeIndex + 1));
            }
            Jedis oldJedis = affectedNode.getJedis();
            Long newNodeHash = Hasher.getHash(node);
//Get all the keys from this(old) node
            oldJedis.connect();
            Set<String> oldKeySet = oldJedis.keys("*");
            if (!oldKeySet.isEmpty()) {
                for (String currentKey : oldKeySet) {
                    if ((Hasher.getHash(currentKey) < newNodeHash) ||
//Checking if data is set in node 0 and has a hash greater than the last node in the node map
                            (lastNodeHash != null && Hasher.getHash(currentKey) > lastNodeHash)) {
//This key is to be moved
//Migrating from the old node to the new node
                        System.out.println("Moving key :" + currentKey + " from " + affectedNode.toString() +
                                " to :" + node.toString());
//Migrate data to the new node
                        if (!node.equals(affectedNode)) {
                            try {
                                oldJedis.migrate(ipAddress, port, currentKey, 0, 5000);
                                migratedKeys.add(new KeyDetailsJSON(currentKey, oldJedis.get(currentKey), affectedNode.getIpAddress(), affectedNode.getPort()));
                            } catch (Exception e) {
                                e.printStackTrace();
                                System.out.println("ERROR : Moving key " + currentKey + " from " + affectedNode.toString() + "(" + replicaHash + ")" + " to " + node.toString());
                                throw new Exception("ERROR : Moving key " + currentKey + " from " + affectedNode.toString() + "(" + replicaHash + ")" + " to " + node.toString());
                            }
                        }
                    }
                }
            }
            nodeMap.put(Hasher.getHash(node), node);
            oldJedis.disconnect();
            oldJedis.close();
//Add the replicas to the cluster.
            if (noOfReplicas > 0) {
                lastNodeHash = null;
                for (int i = 1; i <= noOfReplicas; i++) {
                    replicaHash = Hasher.getReplicaHash(node, i);
                    nodeHashSet.add(replicaHash);
                    Collections.sort(nodeHashSet);
                    replicaIndex = nodeHashSet.indexOf(replicaHash);
                    if (replicaIndex == nodeHashSet.size() - 1) {
                        affectedNode = nodeMap.get(nodeHashSet.get(0));
//Checking for all data pairs whose hash values are greater than last node's hash and are assigned to node 0
                        lastNodeHash = nodeHashSet.get(nodeHashSet.size() - 1);
                    } else {
                        affectedNode = nodeMap.get(nodeHashSet.get(replicaIndex + 1));
                    }
                    oldJedis = affectedNode.getJedis();
//Get all the keys from this(old) node
                    oldJedis.connect();
                    oldKeySet = oldJedis.keys("*");
                    if (!oldKeySet.isEmpty()) {
                        for (String currentKey : oldKeySet) {
                            if ((Hasher.getHash(currentKey) < replicaHash) ||
//Checking if data is set in node 0 and has a hash greater than the last node in the node map
                                    (lastNodeHash != null && Hasher.getHash(currentKey) > lastNodeHash)) {
//This key is to be moved
//Migrating from the old node to the new node(if both are not the same)
                                if (!node.equals(affectedNode)) {
                                    System.out.println("Moving key :" + currentKey + " from " + affectedNode.toString() +
                                            " to :" + node.toString());
//Migrate data to the new node
                                    try {
                                        oldJedis.migrate(ipAddress, port, currentKey, 0, 5000);
                                        migratedKeys.add(new KeyDetailsJSON(currentKey, oldJedis.get(currentKey), affectedNode.getIpAddress(), affectedNode.getPort()));
                                        System.out.println("Migrated");
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        System.out.println("Error :Moving key " + currentKey + " from " + affectedNode.toString() + "(" + replicaHash + ")" + " to " + node.toString());
                                        throw new Exception("ERROR : Moving key " + currentKey + " from " + affectedNode.toString() + "(" + replicaHash + ")" + " to " + node.toString());
                                    }
                                }
                            }
                        }
                    }
                    nodeMap.put(replicaHash, node);
                }
                oldJedis.disconnect();
                oldJedis.close();
            }
        } else {
            throw new Exception("The node at IP : " + ipAddress + ":" + port + " is not up.");
        }
        return migratedKeys;
    }

    public Map<Node, List<RedisData>> getAllData() {
//TODO Return data from all the Redis nodes
        Map<Node, List<RedisData>> outputMap = null;
        Node currentNode = null;
        for (Long hash : nodeHashSet) {
            if (outputMap == null) {
                outputMap = new HashMap<Node, List<RedisData>>();
            }
            currentNode = nodeMap.get(hash);
            outputMap.put(currentNode, getDataFromNode(currentNode));
        }
        if (outputMap != null) {
            return outputMap;
        } else {
            throw new NullPointerException(" There are no nodes available in the cluster. ");
        }
    }

    public List<RedisData> getDataFromNode(Node node) {
//TODO Return data from a single Redis node
        List<RedisData> outputList = null;
        Jedis currJedis = node.getJedis();
        currJedis.connect();
        if (currJedis.isConnected()) {
            outputList = new ArrayList<RedisData>();
            Set<String> keys = currJedis.keys("*");
            for (String key : keys) {
                outputList.add(new RedisData(key, currJedis.get(key)));
            }
            currJedis.disconnect();
            currJedis.close();
        } else {
            throw new NullPointerException("Unable to connect to instance :" + node.toString());
        }
        return outputList;
    }

    public boolean checkIfNodeIsUp(Node node) {
        boolean isConnected = false;
        if (node.getJedis().isConnected()) {
            return true;
        } else {
            try {
                node.getJedis().connect();
                isConnected = node.getJedis().isConnected();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                node.getJedis().disconnect();
                node.getJedis().close();
            }
        }
        return isConnected;
    }

    public Node findNodeForData(RedisData data) {
//TODO Find the node to which this key is to be assigned to
        Long keyHash = Hasher.getHash(data);
        for (Long hash : nodeHashSet) {
            if (keyHash < hash) {
//This is the node in which data will be inserted
                Node assignedNode = (Node) nodeMap.get(hash);
                System.out.println("Hash values : Data : " + keyHash + " Node : " + hash);
                return assignedNode;
            }
        }
        return nodeMap.get(nodeHashSet.get(0));
    }

    public Node insertData(RedisData data) throws NullPointerException {
//TODO insert data into the assigned node and return the node to which it is assigned to
        Node assignedNode = null;
        if (nodeMap != null && !nodeMap.isEmpty()) {
            assignedNode = findNodeForData(data);
            Jedis currJedis = assignedNode.getJedis();
//Connect to the node and perform insertion
            currJedis.connect();
            System.out.println("Key : " + data.getKey() + "(" + Hasher.getHash(data) + ") sent to node : " + assignedNode.toString() + "(" + Hasher.getHash(assignedNode) + ")");
            currJedis.set(data.getKey(), data.getValue().toString());
            currJedis.disconnect();
            currJedis.close();
        }
        if (assignedNode != null) {
            return assignedNode;
        } else {
            throw new NullPointerException("No node available for Key : " + data.toString());
        }
    }

    public List<KeyDetailsJSON> removeFromCluster(String ipAddress, int port) throws Exception {
        Node toBeRemovedNode = nodeMap.get(Hasher.getHash(new Node(ipAddress, port)));
        List<KeyDetailsJSON> migratedKeys = null;
        List<Long> removedHashes = new ArrayList<Long>();
        if (checkIfNodeIsUp(toBeRemovedNode)) {
//Check if it is the only node in the cluster cannot remove it
            if (distinctNodes.size() == 1) {
                System.out.println("ERROR:Cannot Remove The Only Node in the Cluster");
                return migratedKeys;
            }
            Jedis oldJedis = toBeRemovedNode.getJedis();
//Get all the keys from this(old) node
            oldJedis.connect();
            migratedKeys = new ArrayList<KeyDetailsJSON>();
//Retrieve the data from this node
            List<RedisData> reassignedData = getDataFromNode(toBeRemovedNode);
//Remove this node and its replicas from the map
            for (int i = 0; i < nodeHashSet.size(); i++) {
                if (nodeMap.get(nodeHashSet.get(i)).equals(toBeRemovedNode)) {
                    nodeMap.remove(nodeHashSet.get(i));
                    distinctNodes.remove(nodeHashSet.get(i));
                    removedHashes.add(nodeHashSet.get(i));
                }
            }
            nodeHashSet.removeAll(removedHashes);
//Add the data into the cluster
            for (RedisData data : reassignedData) {
                insertData(data);
                Node assignedNode = findNodeForData(data);
                migratedKeys.add(new KeyDetailsJSON(data.getKey(), data.getValue().toString(), assignedNode.getIpAddress(), assignedNode.getPort()));
            }
            oldJedis.flushAll();
            oldJedis.disconnect();
            oldJedis.close();
        } else {
            throw new Exception("The node at IP : " + toBeRemovedNode.toString() + " is not up.");
        }
        return migratedKeys;
    }

    public void flush() {
        for (Node node : distinctNodes.values()) {
            node.getJedis().flushAll();
        }
        nodeHashSet.clear();
        nodeMap.clear();
        distinctNodes.clear();
    }

    public boolean deleteData(String key, int db) throws Exception {
        try {
            Node node = findNodeForData(new RedisData(key, null));
            Jedis jedis = node.getJedis();
            jedis.connect();
            jedis.select(db);
            jedis.del(key);
            jedis.disconnect();
            return true;
        } catch (Exception e) {
// TODO Auto-generated catch block
            e.printStackTrace();
            throw e;
        }
    }


    public Node hset(RedisData data, String field, int db) throws NullPointerException {
//TODO insert data into the assigned node and return the node to which it is assigned to
        Node assignedNode = null;
        if (nodeMap != null && !nodeMap.isEmpty()) {
            assignedNode = findNodeForData(data);
            Jedis currJedis = assignedNode.getJedis();
            currJedis.select(db);
//Connect to the node and perform insertion
            currJedis.connect();
            System.out.println("Key : " + data.getKey() + "(" + Hasher.getHash(data) + ") sent to node : " + assignedNode.toString() + "(" + Hasher.getHash(assignedNode) + ")");
            currJedis.hset(data.getKey(), field, data.getValue().toString());
//            currJedis.set(data.getKey(), data.getValue().toString());
            currJedis.disconnect();
            currJedis.close();
        }
        if (assignedNode != null) {
            return assignedNode;
        } else {
            throw new NullPointerException("No node available for Key : " + data.toString());
        }
    }

    // 1: hgetAll
    public Map<String, String> hgetAll(String key, int db) {
        int color = HashTable.getColor(key);
        RedisData data = new RedisData(key, "", color);
        Node node = findNodeForData(data);
        Jedis jedis = node.getJedis();
        if (nodeMap != null && !nodeMap.isEmpty()) {

            jedis.connect();

            jedis.select(db);
            Map<String, String> result = jedis.hgetAll(key);
            jedis.disconnect();
            return result;
        } else {
            throw new NullPointerException("There are no nodes available in the cluster");
        }
    }

    public boolean hexists(String key, String field, int db) {
        int color = HashTable.getColor(key);
        RedisData data = new RedisData(key, "", color);
        Node node = findNodeForData(data);
        Jedis jedis = node.getJedis();
        if (nodeMap != null && !nodeMap.isEmpty()) {

            jedis.connect();
            jedis.select(db);
            boolean result = jedis.hexists(key, field);
            jedis.disconnect();
            return result;
        } else {
            throw new NullPointerException("There are no nodes available in the cluster");
        }
    }

    public String hget(String key, String field, int db) {
        int color = HashTable.getColor(key);
        RedisData data = new RedisData(key, "", color);
        Node node = findNodeForData(data);
        Jedis jedis = node.getJedis();
        if (nodeMap != null && !nodeMap.isEmpty()) {

            jedis.connect();
            jedis.select(db);
            String result = jedis.hget(key, field);
            jedis.disconnect();
            return result;
        } else {
            throw new NullPointerException("There are no nodes available in the cluster");
        }

    }

    public void lpush(String key, String[] value, int db) {
        int color = HashTable.getColor(key);
        RedisData data = new RedisData(key, "", color);
        Node node = findNodeForData(data);
        Jedis jedis = node.getJedis();
        if (nodeMap != null && !nodeMap.isEmpty()) {

            jedis.connect();
            jedis.select(db);
            jedis.lpush(key, value);
            jedis.disconnect();
        } else {
            throw new NullPointerException("There are no nodes available in the cluster");
        }

    }

    public List<String> lrange(String key, long start, long end, int db) {

        int color = HashTable.getColor(key);
        RedisData data = new RedisData(key, "", color);
        Node node = findNodeForData(data);
        Jedis jedis = node.getJedis();
        if (nodeMap != null && !nodeMap.isEmpty()) {

            jedis.connect();
            jedis.select(db);
            List<String> result = jedis.lrange(key, start, end);
            jedis.disconnect();
            return result;
        } else {
            throw new NullPointerException("There are no nodes available in the cluster");
        }
    }

    public void flushDB(int db) {

    }

    public Set<String> keys(String key, int db) {
        int color = HashTable.getColor(key);
        RedisData data = new RedisData(key, "", color);
        Node node = findNodeForData(data);
        Jedis jedis = node.getJedis();
        if (nodeMap != null && !nodeMap.isEmpty()) {

            jedis.connect();
            jedis.select(db);
            Set<String> result = jedis.keys(key);
            jedis.disconnect();
            return result;
        } else {
            throw new NullPointerException("There are no nodes available in the cluster");
        }
    }

    // 2:


}
