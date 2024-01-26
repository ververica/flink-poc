package org.apache.flink.runtime.state.async;

public class RecordContext<K, R> {

    private final R record;

    private final K key;

    private int refCount;

    private boolean heldStateAccessToken;

    private final BatchingComponent<R, K> batchingComponentHandle;

    public RecordContext(R record, K key, BatchingComponent<R, K> batchingComponentHandle) {
        this.record = record;
        this.key = key;
        this.heldStateAccessToken = false;
        this.refCount = 0;
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

    public void retainRef() {
        ++refCount;
    }

    public void releaseRef() {
        if (--refCount == 0) {
            batchingComponentHandle.releaseStateAccessToken(record, key);
        }
    }
}
