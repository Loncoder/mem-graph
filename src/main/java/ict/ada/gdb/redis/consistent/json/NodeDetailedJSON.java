package ict.ada.gdb.redis.consistent.json;

import ict.ada.gdb.redis.consistent.resources.RedisData;

import java.util.List;

/**
 * Created by lon on 17-2-10.
 */
public class NodeDetailedJSON {

    public NodeDetailedJSON() {
    }

    public NodeDetailedJSON(String ipAddress, int port, List<RedisData> data) {
        super();
        this.ipAddress = ipAddress;
        this.port = port;
        this.data = data;
    }

    public String ipAddress;
    public int port;
    public List<RedisData> data;

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public List<RedisData> getData() {
        return data;
    }

    public void setData(List<RedisData> data) {
        this.data = data;
    }
}
