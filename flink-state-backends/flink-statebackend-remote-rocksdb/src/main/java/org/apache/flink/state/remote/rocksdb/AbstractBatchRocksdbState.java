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
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;

import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.io.IOException;

/**
 * The abstract class for rocksdb batch State.
 */
public abstract class AbstractBatchRocksdbState<K, N, V> implements InternalKvState<K, N, V>, State {

    protected TypeSerializer<K> keySerializer;

    /** Serializer for the namespace. */
    protected TypeSerializer<N> namespaceSerializer;

    /** Serializer for the state values. */
    protected TypeSerializer<V> valueSerializer;

    /** The column family of this particular instance of state. */
    protected final ColumnFamilyHandle columnFamily;

    protected V defaultValue;

    protected final WriteOptions writeOptions;

    protected final RemoteRocksDBKeyedStateBackend<K> backend;

    protected final RocksDB db;

    protected BatchParallelIOExecutor<K> parallelIOExecutor;

    private final ThreadLocal<SerializedCompositeKeyBuilder<K>> sharedKeyNamespaceSerializer;

    protected final ThreadLocal<DataInputDeserializer> dataInputView;

    protected final ThreadLocal<DataOutputSerializer> dataOutputView;

    private final int maxParallelism;

    protected AbstractBatchRocksdbState(
            RemoteRocksDBKeyedStateBackend<K> backend,
            ColumnFamilyHandle columnFamily,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue) {
        this.backend = backend;
        this.db = backend.getDB();
        this.columnFamily = columnFamily;
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer =
                Preconditions.checkNotNull(valueSerializer, "State value serializer");
        this.defaultValue = defaultValue;
        this.writeOptions = backend.getWriteOptions();
        this.dataOutputView = ThreadLocal.withInitial(() -> new DataOutputSerializer(128));
        this.dataInputView = ThreadLocal.withInitial(DataInputDeserializer::new);
        this.maxParallelism = backend.getNumberOfKeyGroups();
        this.sharedKeyNamespaceSerializer = ThreadLocal.withInitial(() ->
                new SerializedCompositeKeyBuilder<>(
                        keySerializer,
                        CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                               maxParallelism),
                        32));
    }


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
        SerializedCompositeKeyBuilder<K> keyBuilder = sharedKeyNamespaceSerializer.get();
        keyBuilder.setKeyAndKeyGroup(key, KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism));
        return keyBuilder.buildCompositeKeyNamespace(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE);
    }

    byte[] serializeValue(V value) throws IOException {
        DataOutputSerializer outputView = dataOutputView.get();
        outputView.clear();
        return serializeValueInternal(outputView, value, valueSerializer);
    }

    private <T> byte[] serializeValueInternal(DataOutputSerializer outputView, T value, TypeSerializer<T> serializer)
            throws IOException {
        serializer.serialize(value, outputView);
        return outputView.getCopyOfBuffer();
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
