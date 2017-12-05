package ict.ada.gdb.redis.consistent.json;

/**
 * Created by lon on 17-2-10.
 */
public class NodeSummaryJSON {
    public NodeSummaryJSON() {
    }

    public NodeSummaryJSON(String ipAddress, int port, int noOfKeys) {
        super();
        this.ipAddress = ipAddress;
        this.port = port;
        this.noOfKeys = noOfKeys;
    }

    public String ipAddress;
    public int port;
    public int noOfKeys;

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

    public int getNoOfKeys() {
        return noOfKeys;
    }

    public void setNoOfKeys(int noOfKeys) {
        this.noOfKeys = noOfKeys;
    }

    @Override
    public String toString() {
// TODO Auto-generated method stub
        return ipAddress + ":" + port + "--" + noOfKeys;
    }
}
