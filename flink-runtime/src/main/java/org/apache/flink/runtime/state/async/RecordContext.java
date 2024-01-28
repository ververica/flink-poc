package org.apache.flink.runtime.state.async;

public class RecordContext<K, R> extends ReferenceCounted {

    private final R record;

    private final K key;

    private boolean heldStateAccessToken;

    private final BatchingComponent<R, K> batchingComponentHandle;

    public RecordContext(R record, K key, BatchingComponent<R, K> batchingComponentHandle) {
        super(0);
        this.record = record;
        this.key = key;
        this.heldStateAccessToken = false;
        this.batchingComponentHandle = batchingComponentHandle;
    }

    public R getRecord() {
        return record;
    }

    public boolean heldStateAccessToken() {
        return heldStateAccessToken;
    }

    public void setHeldStateAccessToken() {
        heldStateAccessToken = true;
    }

    @Override
    protected void referenceCountReachedZero() {
        batchingComponentHandle.releaseStateAccessToken(record, key);
    }
}
