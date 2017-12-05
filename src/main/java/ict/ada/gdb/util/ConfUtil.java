package ict.ada.gdb.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by lon on 16-4-13.
 */
public class ConfUtil {
    public static Configuration getConf(String propsPath) {
        Configuration conf = HBaseConfiguration.create();
        Properties props = new Properties();
        try {
            props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(propsPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (Object key : props.keySet()) {
            conf.set(key.toString(), props.get(key).toString());
        }
        return conf;
    }
}
