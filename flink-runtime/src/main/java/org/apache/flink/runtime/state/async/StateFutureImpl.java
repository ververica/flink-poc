package org.apache.flink.runtime.state.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.async.StateFuture;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.util.function.RunnableWithException;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

@Internal
public class StateFutureImpl<K, V> implements StateFuture<V> {

    private CompletableFuture<V> future;

    private ReferenceCountedKey<K> currentKey;

    private final InternalKeyContext<K> keyContext;

    private final Consumer<RunnableWithException> registerMailBoxCallBackFunc;

    private final Consumer<Integer> updateOngoingStateReq;

    public StateFutureImpl(ReferenceCountedKey<K> currentKey,
                           InternalKeyContext<K> internalKeyContext,
                           Consumer<RunnableWithException> registerMailBoxCallBackFunc,
                           Consumer<Integer> updateOngoingStateReq) {
        this.future = new CompletableFuture<>();
        this.currentKey = currentKey;
        this.keyContext = internalKeyContext;
        this.registerMailBoxCallBackFunc = registerMailBoxCallBackFunc;
        this.updateOngoingStateReq = updateOngoingStateReq;
    }

    public void complete(V value) {
        future.complete(value);
    }


    @Override
    public <R> StateFuture<R> then(Function<? super V, ? extends R> action) {
        currentKey.retain();
        StateFutureImpl<K, R> stateFuture = new StateFutureImpl<>(currentKey, keyContext, registerMailBoxCallBackFunc, updateOngoingStateReq);
        future.thenAccept(value -> {
            updateOngoingStateReq.accept(-1);
            registerMailBoxCallBackFunc.accept(() -> {
                keyContext.setCurrentKey(currentKey, true);
                R result = action.apply(value);
                stateFuture.complete(result);
            });
            currentKey.release();
        });
        return stateFuture;
    }

    @Override
    public StateFuture<Void> then(Consumer<? super V> action) {
        currentKey.retain();
        StateFutureImpl<K, Void> stateFuture = new StateFutureImpl<>(currentKey, keyContext, registerMailBoxCallBackFunc, updateOngoingStateReq);
        future.thenAccept(value -> {
            registerMailBoxCallBackFunc.accept(() -> {
                updateOngoingStateReq.accept(-1);
                keyContext.setCurrentKey(currentKey, true);
                action.accept(value);
                currentKey.release();
                stateFuture.complete(null);
            });
        });
        return stateFuture;
    }
}
