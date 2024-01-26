package org.apache.flink.state.remote.rocksdb.async;

import org.apache.flink.api.common.state.async.AsyncState;
import org.apache.flink.api.common.state.async.StateFuture;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.async.BatchingComponent;
import org.apache.flink.runtime.state.async.InternalAsyncState;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.state.remote.rocksdb.RemoteRocksDBKeyedStateBackend;
import org.apache.flink.state.remote.rocksdb.StateExecutorService;

public class AbstractAsyncRocksdbState<R, K, N, V> implements AsyncState, InternalAsyncState<K, N, V> {

    protected final BatchingComponent<R, K> batchingComponent;
    
    protected final StateExecutorService<K> stateExecutor;

    protected final InternalKeyContext<K> keyContext;

    protected final TypeSerializer<K> keySerializer;
    protected final TypeSerializer<N> namespaceSerializer;
    protected final TypeSerializer<V> valueSerializer;

    AbstractAsyncRocksdbState(
            RemoteRocksDBKeyedStateBackend<R, K> backend,
            InternalKeyContext<K> keyContext,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer) {
        this.batchingComponent = backend.getBatchingComponent();
        this.stateExecutor = backend.getStateExecutorService();
        this.keyContext = keyContext;
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public StateFuture<Void> clear() {
        throw new UnsupportedOperationException();
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

    @Override
    public void setCurrentNamespace(Object namespace) {
        //throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer) throws Exception {
        throw new UnsupportedOperationException();
    }
}
