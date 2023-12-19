package org.apache.flink.state.remote.rocksdb;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.batch.InternalBatchKvState;

public abstract class AbstractBatchRocksdbState<K, N, V> implements InternalBatchKvState<K, N, V>, State {

    /** Serializer for the namespace. */
    TypeSerializer<N> namespaceSerializer;

    /** Serializer for the state values. */
    TypeSerializer<V> valueSerializer;

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
