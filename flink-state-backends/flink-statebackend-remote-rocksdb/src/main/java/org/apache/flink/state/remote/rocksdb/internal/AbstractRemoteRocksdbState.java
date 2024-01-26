package org.apache.flink.state.remote.rocksdb.internal;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.state.remote.rocksdb.RemoteRocksDBKeyedStateBackend;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.io.IOException;

public abstract class AbstractRemoteRocksdbState<K, N, V> implements RemoteRocksdbKVState<K, N, V> {

    protected TypeSerializer<K> keySerializer;

    /** Serializer for the namespace. */
    protected TypeSerializer<N> namespaceSerializer;

    /** Serializer for the state values. */
    protected TypeSerializer<V> valueSerializer;

    /** The column family of this particular instance of state. */
    protected final ColumnFamilyHandle columnFamily;

    private N currentNamespace;

    protected V defaultValue;

    protected final WriteOptions writeOptions;

    protected final RemoteRocksDBKeyedStateBackend<?, K> backend;

    protected final RocksDB db;

    private final ThreadLocal<SerializedCompositeKeyBuilder<K>> sharedKeyNamespaceSerializer;

    protected final ThreadLocal<DataInputDeserializer> dataInputView;

    protected final ThreadLocal<DataOutputSerializer> dataOutputView;

    private final int maxParallelism;

    protected AbstractRemoteRocksdbState(
            RemoteRocksDBKeyedStateBackend<?, K> backend,
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
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
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
}
