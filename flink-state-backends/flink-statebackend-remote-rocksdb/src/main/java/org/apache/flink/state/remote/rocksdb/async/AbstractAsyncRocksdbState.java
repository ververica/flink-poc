package org.apache.flink.state.remote.rocksdb.async;

import org.apache.flink.api.common.state.async.AsyncState;
import org.apache.flink.api.common.state.async.StateFuture;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.async.BatchingComponent;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.state.remote.rocksdb.StateExecutorService;

public class AbstractAsyncRocksdbState<R, K, N, V> implements AsyncState {

    protected final BatchingComponent<R, K> batchingComponent;
    
    protected StateExecutorService<K> stateExecutor;

    protected final InternalKeyContext<K> keyContext;

    AbstractAsyncRocksdbState(
            BatchingComponent<R, K> batchingComponent,
            InternalKeyContext<K> keyContext) {
        this.batchingComponent = batchingComponent;
        this.keyContext = keyContext;
    }

    @Override
    public StateFuture<Void> clear() {
        throw new UnsupportedOperationException();
    }
}
