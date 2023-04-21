package com.olapdb.core.utils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class LruBatchReclaimCache<K,V> extends LinkedHashMap<K,V> {
    private int capacity;
    public LruBatchReclaimCache(int capacity) {
        super(100, 0.75f, true);
        this.capacity = capacity;
    }

    public int getCapacity() {
        return capacity;
    }

    public List<V> trimToSize(int size){
        List<V> removes = new Vector<>();
        while(this.size() > size){
            final Map.Entry<K, V> toEvict = this.entrySet().iterator().next();
            removes.add( this.remove(toEvict.getKey()));
        }

        return removes;
    }
}
