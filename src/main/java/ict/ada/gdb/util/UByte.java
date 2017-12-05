package ict.ada.gdb.util;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * represent 0~255
 * <p/>
 * Created by chenbo on 2015/6/11.
 */
public class UByte {
    public static int MIN_VALUE = 0;
    public static int MAX_VALUE = 255;
    private static Map<Integer, UByte> map = new HashMap<Integer, UByte>();
    private int value;

    private UByte(int value) {
        this.value = value;
    }

    public static UByte get(int v) {
        if (v < 0) v += 256;
        Preconditions.checkArgument(MIN_VALUE<=v && MAX_VALUE>=v,"Invalid UByte Value(Range: 0 ~ 255)");
        if (v < MIN_VALUE || v > MAX_VALUE) {
            throw new RuntimeException("Invalid UByte Value(Range: 0 ~ 255)");
        }
        if (map.containsKey(v)) {
            return map.get(v);
        }
        UByte b = new UByte(v);
        map.put(v, b);
        return b;
    }

    public static UByte get(byte b) {
        int ib = b;
        if (ib < 0) ib += 256;
        return get(ib);
    }

    public static void main(String[] args) {
        System.out.println(UByte.get(8).toHexString());
    }

    public int intValue() {
        return value;
    }

    public byte byteValue() {
        return (byte) (value & 0xff);
    }

    public int hashCode() {
        return value;
    }

    public boolean equals(Object other) {
        if (other == this) return true;
        return (other instanceof UByte && (((UByte) other).value == value));
    }

    public String toString() {
        return String.format("%03d", value);
    }

    public String toHexString() {
        return String.format("%02x", value);
    }
}
