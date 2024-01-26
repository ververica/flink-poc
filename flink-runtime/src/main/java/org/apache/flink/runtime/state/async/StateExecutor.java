package org.apache.flink.runtime.state.async;

import java.util.concurrent.CompletableFuture;

public interface StateExecutor<K> {

    CompletableFuture<Boolean> executeBatchRequests(Iterable<StateRequest<?, K, ?, ?>> stateRequests);
}
