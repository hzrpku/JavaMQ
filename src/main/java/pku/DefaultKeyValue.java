package pku;

import java.util.HashMap;
import java.util.Set;

/**
 * 一个Key-Value的实现
 */
public class DefaultKeyValue implements KeyValue{
    private final HashMap<String, Object> kvs = new HashMap<>(); //kvs为一个HashMap

    public Object getObj(String key) {
        return kvs.get(key);
    }

    public HashMap<String, Object> getMap(){
        return kvs;
    }

    public DefaultKeyValue put(String key, int value) {
        kvs.put(key, value);
        return this;
    }

    public DefaultKeyValue put(String key, long value) {
        kvs.put(key, value);
        return this;
    }

    public DefaultKeyValue put(String key, double value) {
        kvs.put(key, value);
        return this;
    }

    public DefaultKeyValue put(String key, String value) {
        kvs.put(key, value);
        return this;
    }

    public int getInt(String key) {
        return  Integer.parseInt(kvs.getOrDefault(key, 0).toString());
    }

    public long getLong(String key) {
        return Long.parseLong( kvs.getOrDefault(key, 0L).toString());
    }

    public double getDouble(String key) {
        return Double.parseDouble( kvs.getOrDefault(key, 0.0d).toString());
    }

    public String getString(String key) {
        return  kvs.getOrDefault(key, null).toString();
    }

    public Set<String> keySet() {
        return kvs.keySet();
    }

    public boolean containsKey(String key) {
        return kvs.containsKey(key);
    }
}
