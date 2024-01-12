package org.apache.flink.runtime.state.async;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.batch.InternalBatchValueState;
import org.apache.flink.util.function.RunnableWithException;

import java.util.function.BiFunction;

public class AbstractBatchAsyncState<K, N, V, S extends InternalBatchValueState<K, N, V>> implements InternalKvState<K, N, V> {

    protected S original;

    protected final InternalKeyContext<K> keyContext;

    protected final BatchKeyProcessor<K, N, V> batchKeyProcessor;

    AbstractBatchAsyncState(S original, InternalKeyContext<K> keyContext, BiFunction<RunnableWithException, Boolean, Void> registerCallBackFunc) {
        this.original = original;
        this.keyContext = keyContext;
        this.batchKeyProcessor = new BatchKeyProcessor<>(original, keyContext, registerCallBackFunc);
    }

    @Override
    public void clear() {
        original.clear();
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return original.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return original.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return original.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        original.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer) throws Exception {
        return original.getSerializedValue(serializedKeyAndNamespace, safeKeySerializer,
                safeNamespaceSerializer, safeValueSerializer);
    }

    @Override
    public InternalKvState.StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        return original.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }
}
