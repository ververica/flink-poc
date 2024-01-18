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
public class StateFutureImpl<K, V> implements StateFuture<V> {

    private CompletableFuture<V> future;

    private final K currentKey;

    private final InternalKeyContext<K> keyContext;

    private final BiFunction<RunnableWithException, Boolean, Void> registerMailBoxCallBackFunc;

    public StateFutureImpl(K currentKey,
                           InternalKeyContext<K> internalKeyContext,
                           BiFunction<RunnableWithException, Boolean, Void> registerMailBoxCallBackFunc) {
        this.future = new CompletableFuture<>();
        this.currentKey = currentKey;
        this.keyContext = internalKeyContext;
        this.registerMailBoxCallBackFunc = registerMailBoxCallBackFunc;
    }

    public void complete(V value) {
        future.complete(value);
    }


    @Override
    public <R> StateFuture<R> then(Function<? super V, ? extends R> action) {
        StateFutureImpl<K, R> stateFuture = new StateFutureImpl<>(currentKey, keyContext, registerMailBoxCallBackFunc);
        future.thenAccept(value -> registerMailBoxCallBackFunc.apply(() -> {
            keyContext.setCurrentKey(currentKey, true);
            R result = action.apply(value);
            stateFuture.complete(result);
        }, false));
        return stateFuture;
    }

    @Override
    public StateFuture<Void> then(Consumer<? super V> action) {
        StateFutureImpl<K, Void> stateFuture = new StateFutureImpl<>(currentKey, keyContext, registerMailBoxCallBackFunc);
        future.thenAccept(value -> {
            registerMailBoxCallBackFunc.apply(() -> {
                keyContext.setCurrentKey(currentKey, true);
                action.accept(value);
                stateFuture.complete(null);
            }, false);
        });
        return stateFuture;
    }
}
