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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.internal.InternalKvState;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.io.IOException;

/**
 * The abstract class for rocksdb batch State.
 */
public abstract class AbstractBatchRocksdbState<K, N, V> implements InternalKvState<K, N, V>, State {

    /** Serializer for the namespace. */
    TypeSerializer<N> namespaceSerializer;

    /** Serializer for the state values. */
    TypeSerializer<V> valueSerializer;

    /** The column family of this particular instance of state. */
    protected ColumnFamilyHandle columnFamily;

    protected V defaultValue;

    protected WriteOptions writeOptions;

    protected KeyedStateBackend<K> backend;

    protected RocksDB db;

    protected BatchParallelIOExecutor<K> parallelIOExecutor;

    private ThreadLocal<SerializedCompositeKeyBuilder<K>> sharedKeyNamespaceSerializer;

    protected ThreadLocal<DataInputDeserializer> dataInputView;

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    protected V getDefaultValue() {
        if (defaultValue != null) {
            return valueSerializer.copy(defaultValue);
        } else {
            return null;
        }
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    protected byte[] serializeCurrentKeyWithGroupAndNamespace(K key) {
        throw new UnsupportedOperationException();
    }

    byte[] serializeValue(V value) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        throw new UnsupportedOperationException();
    }
}
