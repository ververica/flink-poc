package org.apache.flink.runtime.state.async;

import org.apache.flink.api.common.state.async.Callback;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.async.InternalAsyncValueState;
import org.apache.flink.runtime.state.internal.batch.InternalBatchValueState;
import org.apache.flink.util.function.RunnableWithException;

import java.io.IOException;
import java.util.function.BiFunction;

public class BatchAsyncValueState<K, N, T>
        extends AbstractBatchAsyncState<
        K,
        N,
        T,
        InternalBatchValueState<K, N, T>>
        implements InternalAsyncValueState<K, N, T> {

    BatchAsyncValueState(
            InternalBatchValueState<K, N, T> original,
            InternalKeyContext<K> keyContext,
            BiFunction<RunnableWithException, Boolean, Void> registerCallBackFunc) {
        super(original, keyContext, registerCallBackFunc);
    }

    @Override
    public void value(Callback<T> callback) throws IOException {
        batchKeyProcessor.get(keyContext.getCurrentKey(),
                new InternalStateCallback<>(keyContext.getCurrentKey(), callback, keyContext));
    }

    @Override
    public void update(T value, Callback<Void> callback) throws IOException {
        batchKeyProcessor.put(keyContext.getCurrentKey(), value,
                new InternalStateCallback<>(keyContext.getCurrentKey(), callback, keyContext));
    }

    @Override
    public void commit(){
        batchKeyProcessor.endKeyProcess(keyContext.getCurrentKey());
    }
}
