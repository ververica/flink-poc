package org.apache.flink.runtime.state.async;

import org.apache.flink.api.common.state.async.StateFuture;
import org.apache.flink.api.common.state.async.StateUncheckedIOException;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.async.InternalAsyncValueState;
import org.apache.flink.runtime.state.internal.batch.InternalBatchValueState;
import org.apache.flink.util.function.RunnableWithException;

import java.io.IOException;
import java.util.function.Consumer;

public class BatchAsyncValueState<K, N, T>
        extends AbstractBatchAsyncState<
        K,
        N,
        T,
        InternalBatchValueState<K, N, T>>
        implements InternalAsyncValueState<K, N, T> {

    private final Consumer<Integer> updateOngoingStateReq;

    BatchAsyncValueState(
            InternalBatchValueState<K, N, T> original,
            InternalKeyContext<K> keyContext,
            Consumer<RunnableWithException> registerCallBackFunc,
            Consumer<Integer> updateOngoingStateReq) {
        super(original, keyContext, registerCallBackFunc, updateOngoingStateReq);
        this.updateOngoingStateReq = updateOngoingStateReq;
    }

    @Override
    public StateFuture<T> value() throws StateUncheckedIOException {
        try {
            updateOngoingStateReq.accept(1);
            return batchKeyProcessor.get(keyContext.getCurrentKey().getRawKey());
        } catch (IOException e) {
            throw new StateUncheckedIOException(e);
        }
    }

    @Override
    public StateFuture<Void> update(T value) throws StateUncheckedIOException {
        try {
            updateOngoingStateReq.accept(1);
            return batchKeyProcessor.put(keyContext.getCurrentKey().getRawKey(), value);
        } catch (IOException e) {
            throw new StateUncheckedIOException(e);
        }
    }

    @Override
    public void commit(){
        batchKeyProcessor.endKeyProcess(keyContext.getCurrentKey().getRawKey());
    }
}
