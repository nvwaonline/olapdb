package com.olapdb.core.utils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 固定容量的LRU缓存器
 * @param <K>
 * @param <V>
 */
public class LruCache<K,V> extends LinkedHashMap<K,V> {
    private int capacity;
    public LruCache(int capacity) {
        super(100, 0.75f, true);
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() > capacity;
    }
}
