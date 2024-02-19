package org.apache.flink.runtime.state.async;

import org.apache.flink.api.common.state.async.StateFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * StateFuture for sync execution within async state APIs.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class SyncExeStateFuture<K, V> implements StateFuture<V> {

    private final CompletableFuture<V> future;

    private V value;

    public SyncExeStateFuture() {
        this.future = new CompletableFuture<>();
        this.value = null;
    }

    @Override
    public <U> StateFuture<U> thenApply(Function<? super V, ? extends U> fn) {
        SyncExeStateFuture stateFuture = new SyncExeStateFuture();
        stateFuture.complete(fn.apply(value));
        return stateFuture;
    }

    @Override
    public StateFuture<Void> thenAccept(Consumer<? super V> action) {
        SyncExeStateFuture stateFuture = new SyncExeStateFuture();
        action.accept(value);
        stateFuture.complete(null);
        return stateFuture;
    }

    @Override
    public <U> StateFuture<U> thenCompose(Function<? super V, ? extends StateFuture<U>> action) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <U, V1> StateFuture<V1> thenCombine(StateFuture<? extends U> other, BiFunction<? super V, ? super U, ? extends V1> fn) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V get() {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void complete(V value) {
        this.value = value;
        future.complete(value);
    }
}
