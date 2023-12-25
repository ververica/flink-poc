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
 */

package org.apache.flink.runtime.state.batch;

import org.apache.flink.api.common.state.batch.CommittedValue;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.internal.batch.InternalBatchValueState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BatchCacheValueState<K, N, T>
        extends AbstractBatchCacheState<
        K,
        N,
        T,
        InternalBatchValueState<K, N, T>>
        implements InternalValueState<K, N, T>, KeyedStateBackend.ClearCurrentKeysCacheListener {

    private static final Logger LOG = LoggerFactory.getLogger(BatchCacheValueState.class);
    private final Map<K, CommittedValue<T>> cachedValues = new HashMap<>();

    public BatchCacheValueState(InternalBatchValueState<K, N, T> original,
                                InternalKeyContext<K> keyContext,
                                BatchCacheStateConfig batchCacheStateConfig) {
        super(original, keyContext);
    }

    @Override
    public T value() throws IOException {
        if (cachedValues.containsKey(keyContext.getCurrentKey())) {
            return cachedValues.get(keyContext.getCurrentKey()).getValue();
        }

        // load from stateBackend, and try to fill cache
        Iterable<T> values = original.values();
        tryFillCache(values);

        return cachedValues.get(keyContext.getCurrentKey()).getValue();
    }

    @Override
    public void update(T value) throws IOException {
        cachedValues.put(keyContext.getCurrentKey(), CommittedValue.of(value, CommittedValue.CommittedValueType.UPDATE));
    }

    @Override
    public void clear() {
        cachedValues.put(keyContext.getCurrentKey(), CommittedValue.ofDeletedValue());
    }

    private void tryFillCache(Iterable<T> values) {
        Iterator<K> keyIter = keyContext.getCurrentKeys().iterator();
        for (T value : values) {
            K key = keyIter.next();
            if (!cachedValues.containsKey(key)) {
                cachedValues.put(
                        key,
                        CommittedValue.of(value, CommittedValue.CommittedValueType.UNMODIFIED));
            }
            LOG.debug("try fill cache {}", values);
        }
    }

    public void writeBackCacheData() {
        if (cachedValues.isEmpty()) {
            return;
        }

        List<K> keys = new ArrayList<>(cachedValues.size());
        List<CommittedValue<T>> values = new ArrayList<>(cachedValues.size());
        cachedValues.entrySet().forEach(entry -> {
            keys.add(entry.getKey());
            values.add(entry.getValue());
        });
        try {
            keyContext.setCurrentKeys(keys);
            original.update(values);
            cachedValues.clear();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        LOG.debug("clear cache with keys {} value {}", keys, values);
    }

    @Override
    public void notifyClearCache() {
        writeBackCacheData();
    }
}
