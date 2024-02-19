package org.apache.flink.runtime.state.heap.async;

import org.apache.flink.runtime.state.async.InternalAsyncState;

public abstract class AbstractAsyncHeapState<K, N, SV> implements InternalAsyncState<K, N, SV> {
    public AbstractAsyncHeapState() {}
}
