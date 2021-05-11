package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCacheImpl implements DatabaseCache {
    private LinkedHashMap<String, byte[]> map = new LinkedHashMap<String, byte[]>() {
        private static final int MAX = 1000;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
            return size() > MAX;
        }
    };

    @Override
    public byte[] get(String key) {
        if (!map.containsKey(key)){
            return null;
        }
        return map.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        map.put(key, value);
    }

    @Override
    public void delete(String key) {
        if (map.containsKey(key)){
            map.remove(key);
        }
    }
}
