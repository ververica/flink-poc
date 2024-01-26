package org.apache.flink.state.remote.rocksdb.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;

import org.apache.flink.state.remote.rocksdb.RemoteRocksDBKeyedStateBackend;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.List;

@Internal
public class RemoteRocksdbValueState<K, N, V>
        extends AbstractRemoteRocksdbState<K, N, V> implements RemoteValueState<K, N, V> {

    public RemoteRocksdbValueState(
            RemoteRocksDBKeyedStateBackend<?, K> backend,
            ColumnFamilyHandle columnFamily,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue) {
        super(
                backend,
                columnFamily,
                keySerializer,
                namespaceSerializer,
                valueSerializer,
                defaultValue);
    }

    @Override
    public V get(K key) throws IOException {
        try {
            byte[] valueBytes = db.get(columnFamily, serializeCurrentKeyWithGroupAndNamespace(key));
            if (valueBytes == null) {
                return getDefaultValue();
            }
            DataInputDeserializer deserializeView = dataInputView.get();
            deserializeView.setBuffer(valueBytes);
            return valueSerializer.deserialize(deserializeView);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<V> multiGet(List<K> keys) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(K key, V value) throws IOException {
        try {
            db.put(
                    columnFamily,
                    writeOptions,
                    serializeCurrentKeyWithGroupAndNamespace(key),
                    serializeValue(value));
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
