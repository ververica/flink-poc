package org.apache.flink.runtime.state.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.async.StateFuture;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.util.function.RunnableWithException;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

@Internal
public class StateFutureImpl<R, K, V> implements InternalStateFuture<V> {

    private final CompletableFuture<V> future;

    private final K currentKey;

    private final RecordContext<K, R> currentRecord;

    private final InternalKeyContext<K> keyContext;

    private final Consumer<RunnableWithException> registerMailBoxCallBackFunc;

    public StateFutureImpl(K currentKey,
                           RecordContext<K, R> currentRecord,
                           InternalKeyContext<K> internalKeyContext,
                           Consumer<RunnableWithException> registerMailBoxCallBackFunc) {
        this.future = new CompletableFuture<>();
        this.currentKey = currentKey;
        this.currentRecord = currentRecord;
        this.keyContext = internalKeyContext;
        this.registerMailBoxCallBackFunc = registerMailBoxCallBackFunc;
    }

    @Override
    public void complete(V value) {
        future.complete(value);
    }

    @Override
    public <C> StateFuture<C> then(Function<? super V, ? extends C> action) {
        currentRecord.retainRef();
        StateFutureImpl<R, K, C> stateFuture = new StateFutureImpl<>(currentKey, currentRecord, keyContext, registerMailBoxCallBackFunc);
        future.thenAccept(value -> {
            registerMailBoxCallBackFunc.accept(() -> {
                keyContext.setCurrentKey(currentKey);
                keyContext.setCurrentRecordContext(currentRecord);
                C result = action.apply(value);
                stateFuture.complete(result);
                currentRecord.releaseRef();
            });
        });
        return stateFuture;
    }

    @Override
    public StateFuture<Void> then(Consumer<? super V> action) {
        currentRecord.retainRef();
        StateFutureImpl<R, K, Void> stateFuture = new StateFutureImpl<>(currentKey, currentRecord, keyContext, registerMailBoxCallBackFunc);
        future.thenAccept(value -> {
            registerMailBoxCallBackFunc.accept(() -> {
                keyContext.setCurrentKey(currentKey);
                keyContext.setCurrentRecordContext(currentRecord);
                action.accept(value);
                stateFuture.complete(null);
                currentRecord.releaseRef();
            });
        });
        return stateFuture;
    }

    @Override
    public RecordContext<K, R> getCurrentRecordContext() {
        return currentRecord;
    }
}
