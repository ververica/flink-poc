package org.apache.flink.runtime.state.heap.async;

import org.apache.flink.api.common.state.async.AsyncValueState;
import org.apache.flink.api.common.state.async.StateFuture;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.async.SyncExeStateFuture;
import org.apache.flink.runtime.state.heap.HeapValueState;

public class AsyncHeapValueState<K, N, SV> extends AbstractAsyncHeapState<K, N, SV> implements AsyncValueState<SV> {

    private HeapValueState<K, N, SV> heapValueState;

    public AsyncHeapValueState(HeapValueState<K, N, SV> heapValueState) {
        super();
        this.heapValueState = heapValueState;
    }

    @Override
    public StateFuture<Void> clear() {
        heapValueState.clear();
        return new SyncExeStateFuture<K, Void>();
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return heapValueState.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return heapValueState.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<SV> getValueSerializer() {
        return heapValueState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        heapValueState.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer,
                                     TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<SV> safeValueSerializer) throws Exception {
        return heapValueState.getSerializedValue(serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
    }

    @Override
    public StateFuture<SV> value() {
        SV value = heapValueState.value();
        SyncExeStateFuture future = new SyncExeStateFuture<>();
        future.complete(value);
        return future;
    }

    @Override
    public StateFuture<Void> update(SV value) {
        heapValueState.update(value);
        SyncExeStateFuture future = new SyncExeStateFuture<>();
        future.complete(null);
        return future;
    }
}
