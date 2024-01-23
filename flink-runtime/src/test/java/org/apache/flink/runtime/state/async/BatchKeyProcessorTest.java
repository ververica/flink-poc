/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.async;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;
import org.apache.flink.runtime.state.internal.batch.InternalBatchValueState;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

class BatchKeyProcessorTest {
    @Test
    void testKeyConflict() throws IOException {
        TestingBatchValueState<Integer, Integer, Integer> testingBatchValueState = new TestingBatchValueState<>();
        InternalKeyContextImpl<Integer> keyContext = new InternalKeyContextImpl<>(new KeyGroupRange(0, 9), 10);
        BatchKeyProcessor<Integer, Integer, Integer> batchKeyProcessor = new BatchKeyProcessor(testingBatchValueState,
                keyContext,
                (v) -> {},
                (v) -> {},
                1,
                2);
        // todo
    }

    class TestingBatchValueState<K, N, V> implements InternalBatchValueState<K, N, V> {
        private HashMap<K, V> state = new HashMap<>();

        @Override
        public void clear() {
            state.clear();
        }

        @Override
        public V value() throws IOException {
            return null;
        }

        @Override
        public void update(V value) throws IOException {

        }

        @Override
        public TypeSerializer<K> getKeySerializer() {
            return null;
        }

        @Override
        public TypeSerializer<N> getNamespaceSerializer() {
            return null;
        }

        @Override
        public TypeSerializer<V> getValueSerializer() {
            return null;
        }

        @Override
        public void setCurrentNamespace(N namespace) {
        }

        @Override
        public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<V> safeValueSerializer) throws Exception {
            return new byte[0];
        }

        @Override
        public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
            return null;
        }

        @Override
        public Iterable<Tuple2<K, V>> values(Collection<K> keys) throws IOException {
            return keys.stream().map(k -> new Tuple2<>(k, state.get(k))).collect(java.util.stream.Collectors.toList());
        }

        @Override
        public void update(K key, V value) throws IOException {
            state.put(key, value);
        }
    }
}
