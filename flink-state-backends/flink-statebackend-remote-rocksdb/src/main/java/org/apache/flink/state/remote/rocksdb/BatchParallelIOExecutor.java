/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.remote.rocksdb;

import org.apache.flink.util.concurrent.FutureUtils;

import org.rocksdb.RocksDBException;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 * The executor for batch parallel IO operation.
 */
public class BatchParallelIOExecutor<K> implements Closeable {

    private final ExecutorService asyncIOExecutor;

    public BatchParallelIOExecutor(ExecutorService asyncIOExecutor) {
        this.asyncIOExecutor = asyncIOExecutor;
    }

    public <V> Iterable<V> fetchValues(Collection<K> keys, FetchRocksdbDataFunction<K, V> fetchFunc) throws IOException {
        Iterator<K> keyIter = keys.iterator();
        ValueArray<V> result = new ValueArray<>(keys.size()); //TODO reuse
        List<CompletableFuture<Void>> futures = new ArrayList<>(keys.size());
        int iterIndex = 0;
        while (keyIter.hasNext()) {
            final K key = keyIter.next();
            final int keyIndex = iterIndex;
            CompletableFuture<Void> singleResult =
                CompletableFuture.runAsync(() -> {
                    try {
                        result.set(keyIndex, fetchFunc.apply(key));
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                }, asyncIOExecutor);
            futures.add(singleResult);
            iterIndex++;
        }
        try {
            FutureUtils.completeAll(futures).get();
        } catch (InterruptedException | ExecutionException | CompletionException e) {
            //TODO handle rocksdbException gracefully
            throw new IOException(e);
        }

        return result;
    }

    @Override
    public void close() throws IOException {
        asyncIOExecutor.shutdownNow();
    }

    public interface FetchRocksdbDataFunction<K, V> {
        V apply(K key) throws RocksDBException, IOException;
    }

    static class ValueArray<V> implements Iterable<V> {
        private Object[] array;

        ValueArray(int length) {
            this.array = new Object[length];
        }

        public void set(int index, V value) {
            array[index] = value;
        }

        @Override
        public Iterator<V> iterator() {
            return new Iterator<V>() {
                int index = 0;

                @Override
                public boolean hasNext() {
                    return index < array.length;
                }

                @Override
                public V next() {
                    return (V) array[index++];
                }
            };
        }
    }
}


