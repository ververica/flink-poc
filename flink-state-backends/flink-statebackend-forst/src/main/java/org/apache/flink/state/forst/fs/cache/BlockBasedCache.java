/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.state.forst.fs.cache;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class BlockBasedCache extends LruCache<Tuple3<Path, Long, Integer>, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(BlockBasedCache.class);

    private final AtomicLong cacheHit = new AtomicLong(0L);

    private final AtomicLong cacheMiss = new AtomicLong(0L);

    private final AtomicLong cacheSize = new AtomicLong(0L);

    public BlockBasedCache(int capacity, long sizeLimit, MetricGroup metricGroup) {
        super(capacity, sizeLimit);
        if (metricGroup != null) {
            metricGroup.gauge("hit", cacheHit::get);
            metricGroup.gauge("miss", cacheMiss::get);
            metricGroup.gauge("size", cacheSize::get);
        }
    }

    @Override
    byte[] internalGet(Tuple3<Path, Long, Integer> key, byte[] value) {
        if (value != null) {
            cacheHit.incrementAndGet();
        } else {
            cacheMiss.incrementAndGet();
        }
        return value;
    }

    @Override
    void internalInsert(Tuple3<Path, Long, Integer> key, byte[] value) {
        cacheSize.addAndGet(value.length);
    }

    @Override
    void internalRemove(byte[] value) {
        cacheSize.addAndGet(-value.length);
    }

    @Override
    void internalClose(byte[] value) {
        cacheSize.addAndGet(-value.length);
    }

    @Override
    int getValueResource(byte[] value) {
        return value.length;
    }
}

/**
 * Uniformed LRU Cache.
 * @param <K> key type.
 * @param <V> value type.
 */
abstract class LruCache<K, V> implements Closeable {
    protected long resource;

    private final LruHashMap dataMap;

    protected long currentResource;

    /**
     * Internal underlying data map.
     */
    class LruHashMap extends LinkedHashMap<K, V> {

        private static final int DEFAULT_SIZE = 1024;

        private final int capacity;

        LruHashMap(int capacity) {
            super(DEFAULT_SIZE, 0.75f, true);
            this.capacity = capacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> entry) {
            if (capacity > 0 && size() > capacity) {
                internalRemove(entry.getValue());
                currentResource -= getValueResource(entry.getValue());
                return true;
            }
            return false;
        }
    }

    LruCache(int capacity, long resource) {
        this.resource = resource;
        this.dataMap = new LruHashMap(capacity);
        this.currentResource = 0;
    }

    public long getCurrentResource() {
        return currentResource;
    }

    public boolean put(K key, V value) {
        if (getValueResource(value) > 64 * 1024L) {
            return false;
        }
        V previous = dataMap.put(key, value);
        if (previous != null) {
            internalRemove(previous);
            currentResource -= getValueResource(previous);
        }
        internalInsert(key, value);
        currentResource += getValueResource(value);
        trimToSize();
        return false;
    }

    public V get(K key) {
        V value = dataMap.get(key);
        return internalGet(key, value);
    }

    public V remove(K key) {
        V previous = dataMap.remove(key);
        if (previous != null) {
            internalRemove(previous);
            currentResource -= getValueResource(previous);
        }
        return previous;
    }

    public int getSize() {
        return dataMap.size();
    }

    private void trimToSize() {
        if (resource <= 0) {
            return;
        }
        while (currentResource > resource && !dataMap.isEmpty()) {
            Map.Entry<K, V> toRemove = dataMap.entrySet().iterator().next();
            dataMap.remove(toRemove.getKey());
            internalRemove(toRemove.getValue());
            currentResource -= getValueResource(toRemove.getValue());
        }
    }

    public long getResource() {
        return resource;
    }

    @Override
    public void close() {
        for (V value : dataMap.values()) {
            internalClose(value);
        }
    }

    abstract V internalGet(K key, V value);

    abstract void internalInsert(K key, V value);

    abstract void internalRemove(V value);

    abstract void internalClose(V value);

    abstract int getValueResource(V value);
}
