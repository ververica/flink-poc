package org.apache.flink.state.remote.rocksdb;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.batch.InternalBatchValueState;

import java.io.IOException;

/**
 * BatchRocksdbValueState.
 */
public class BatchRocksdbValueState<K, N, V> extends AbstractBatchRocksdbState<K, N, V>
        implements InternalBatchValueState<K, N, V> {


    @Override
    public Iterable<V> values() throws IOException {
       throw new UnsupportedOperationException();
    }

    @Override
    public void update(Iterable<V> values) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TypeSerializer getKeySerializer() {
        throw new UnsupportedOperationException();
    }
}
