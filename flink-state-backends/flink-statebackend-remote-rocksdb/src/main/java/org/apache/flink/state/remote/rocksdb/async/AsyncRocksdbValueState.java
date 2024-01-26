package org.apache.flink.state.remote.rocksdb.async;

import org.apache.flink.api.common.state.async.AsyncState;
import org.apache.flink.api.common.state.async.AsyncStateDescriptor;
import org.apache.flink.api.common.state.async.AsyncValueState;
import org.apache.flink.api.common.state.async.StateFuture;
import org.apache.flink.api.common.state.async.StateUncheckedIOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.async.InternalAsyncState;
import org.apache.flink.runtime.state.async.StateFutureImpl;
import org.apache.flink.runtime.state.async.StateRequest;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.state.remote.rocksdb.RemoteRocksDBKeyedStateBackend;
import org.apache.flink.state.remote.rocksdb.internal.RemoteRocksdbValueState;

import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;

public class AsyncRocksdbValueState<R, K, N, V>
        extends AbstractAsyncRocksdbState<R, K, N, V> implements AsyncValueState<V> {

    private RemoteRocksdbValueState<K, N, V> syncValueState;

    AsyncRocksdbValueState(
            RemoteRocksDBKeyedStateBackend<R, K> backend,
            InternalKeyContext<K> keyContext,
            ColumnFamilyHandle columnFamily,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue) {
        super(backend.getBatchingComponent(), keyContext);
        this.syncValueState = new RemoteRocksdbValueState<>(backend, columnFamily, keySerializer,
                namespaceSerializer, valueSerializer, defaultValue);
        stateExecutor.registerState(syncValueState);
    }

    @Override
    public StateFuture<V> value() {
        StateFutureImpl<R, K, V> future = batchingComponent.newStateFuture(
                keyContext.getCurrentKey(), keyContext.getCurrentRecordContext(), keyContext);
        StateRequest<RemoteRocksdbValueState<K, N, V>, K, Void, V> valueGet =
                StateRequest.ofValueGet(syncValueState, keyContext.getCurrentKey(), future);
        try {
            batchingComponent.processStateRequest(valueGet, keyContext.getCurrentRecordContext());
            return future;
        } catch (IOException e) {
            throw new StateUncheckedIOException(e);
        }
    }

    @Override
    public StateFuture<Void> update(V value) {
        StateFutureImpl<R, K, Void> future = batchingComponent.newStateFuture(
                keyContext.getCurrentKey(), keyContext.getCurrentRecordContext(), keyContext);
        StateRequest<RemoteRocksdbValueState<K, N, V>, K, V, Void> valuePut =
                StateRequest.ofValuePut(syncValueState, keyContext.getCurrentKey(), value, future);
        try {
            batchingComponent.processStateRequest(valuePut, keyContext.getCurrentRecordContext());
            return future;
        } catch (IOException e) {
            throw new StateUncheckedIOException(e);
        }
    }

    public static <R, K, N, SV, S extends AsyncState, IS extends S> IS create(
            AsyncStateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            RemoteRocksDBKeyedStateBackend<R, K> backend) {
        return (IS)
                new AsyncRocksdbValueState<>(
                        backend,
                        backend.getKeyContext(),
                        registerResult.f0,
                        backend.getKeySerializer(),
                        registerResult.f1.getNamespaceSerializer(),
                        registerResult.f1.getStateSerializer(),
                        stateDesc.getDefaultValue());
    }
}
