package org.apache.flink.runtime.state.batch;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;

public abstract class AbstractBatchCacheState<
        K, N, V, S extends InternalKvState<K, N, V>> implements InternalKvState<K, N, V>, KeyedStateBackend.ClearCurrentKeysCacheListener {

    protected S original;

    protected final InternalKeyContext<K> keyContext;

    AbstractBatchCacheState(S original, InternalKeyContext<K> keyContext) {
        this.original = original;
        this.keyContext = keyContext;
    }

    @Override
    public void clear() {
        original.clear();;
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
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        return original.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }
}
