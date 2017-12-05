package ict.ada.gdb.redis.consistent.json;

/**
 * Created by lon on 17-2-10.
 */
public class KeyDetailsJSON {
    public KeyDetailsJSON() {
// TODO Auto-generated constructor stub
    }

    public KeyDetailsJSON(String key, String value, String sourceIPAddress, int sourcePort) {
        super();
        this.key = key;
        this.value = value;
        this.sourceIPAddress = sourceIPAddress;
        this.sourcePort = sourcePort;
    }

    public String key;
    public String value;
    public String sourceIPAddress;
    public int sourcePort;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getSourceIPAddress() {
        return sourceIPAddress;
    }

    public void setSourceIPAddress(String sourceIPAddress) {
        this.sourceIPAddress = sourceIPAddress;
    }

    public int getSourcePort() {
        return sourcePort;
    }

    public void setSourcePort(int sourcePort) {
        this.sourcePort = sourcePort;
    }
}
