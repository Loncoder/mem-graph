package ict.ada.gdb.redis.consistent.resources;

/**
 * Created by lon on 17-2-10.
 */
public class RedisData implements IHashable {
    private String key;
    private Object value;
    private int color;

    public RedisData(String key, Object value) {
        this(key, value, 0);
    }

    public RedisData(String key, Object value, int color) {
        super();
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
// TODO Auto-generated method stub
        return key + ":" + value;
    }

    public int getColor() {
        return color;
    }

    public void setColor(int color) {
        this.color = color;
    }
}
