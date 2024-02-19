package org.apache.flink.contrib.streaming.state.async;

import org.apache.flink.runtime.state.async.InternalAsyncState;

public abstract class AbstractAsyncRocksDBState<K, N, SV> implements InternalAsyncState<K, N, SV> {

    public AbstractAsyncRocksDBState() {}
}
