package org.apache.flink.runtime.state.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.async.AsyncState;
import org.apache.flink.api.common.state.async.StateFuture;
import org.apache.flink.runtime.state.heap.InternalKeyContext;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.state.async.StateRequest.RequestType.GET;
import static org.apache.flink.runtime.state.async.StateRequest.RequestType.PUT;

@Internal
public class StateRequest<S, K, V, F> {

    S state;

    RequestType type;

    K key;

    @Nullable V value;

    InternalStateFuture<F> stateFuture;

    private StateRequest(S state, RequestType type, K key, @Nullable V value, InternalStateFuture<F> stateFuture) {
        this.state = state;
        this.type = type;
        this.key = key;
        this.value = value;
        this.stateFuture = stateFuture;
    }

    public S getState() {
        return state;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public RequestType getQuestType() {
        return type;
    }
    
    public InternalStateFuture<F> getFuture() {
        return stateFuture;
    }

    public static <S, K, V> StateRequest<S, K, Void, V> ofValueGet(
            S state, K key, InternalStateFuture<V> future) {
        return new StateRequest<>(state, GET, key, null, future);
    }

    public static <S, K, V> StateRequest<S, K, V, Void> ofValuePut(S state, K key, V value, InternalStateFuture<Void> future) {
        return new StateRequest<>(state, PUT, key, value, future);
    }


    public enum RequestType {
        GET, PUT
    }
}
