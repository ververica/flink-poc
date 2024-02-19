package org.apache.flink.contrib.streaming.state.async;

import org.apache.flink.api.common.state.async.AsyncValueState;
import org.apache.flink.api.common.state.async.StateFuture;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBValueState;
import org.apache.flink.runtime.state.async.SyncExeStateFuture;

import java.io.IOException;

public class AsyncRocksDBValueState<K, N, SV> extends AbstractAsyncRocksDBState<K, N, SV> implements AsyncValueState<SV> {
    private RocksDBValueState<K, N, SV> rocksdbValueState;

    public AsyncRocksDBValueState(RocksDBValueState<K, N, SV> rocksdbValueState) {
        super();
        this.rocksdbValueState = rocksdbValueState;
    }

    @Override
    public StateFuture<Void> clear() {
        rocksdbValueState.clear();
        return new SyncExeStateFuture<K, Void>();
    }

    @Override
    public StateFuture<SV> value() {
        try {
            SV value = rocksdbValueState.value();
            SyncExeStateFuture future = new SyncExeStateFuture<>();
            future.complete(value);
            return future;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StateFuture<Void> update(SV value) {
        try {
            rocksdbValueState.update(value);
            SyncExeStateFuture future = new SyncExeStateFuture<>();
            future.complete(null);
            return future;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return rocksdbValueState.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return rocksdbValueState.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<SV> getValueSerializer() {
        return rocksdbValueState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        rocksdbValueState.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer,
                                     TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<SV> safeValueSerializer) throws Exception {
        return rocksdbValueState.getSerializedValue(serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
    }
}
