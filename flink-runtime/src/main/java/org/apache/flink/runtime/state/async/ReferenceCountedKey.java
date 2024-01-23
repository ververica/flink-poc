package org.apache.flink.runtime.state.async;

import org.apache.flink.runtime.state.ReferenceCounted;

import java.util.function.Consumer;

public class ReferenceCountedKey<K> extends ReferenceCounted {
    private final K rawKey;

    private Consumer<K> releaseKeyFunc;

    public ReferenceCountedKey(int initRef, K originKey) {
        super(initRef);
        this.rawKey = originKey;
    }

    public K getRawKey() {
        return rawKey;
    }

    public void setReleaseKeyFunc(Consumer<K> releaseKeyFunc) {
        this.releaseKeyFunc = releaseKeyFunc;
    }

    @Override
    protected void referenceCountReachedZero() {
        releaseKeyFunc.accept(rawKey);
    }

    public int retain() {
        return super.retain();
    }

    public int release() {
        return super.release();
    }

    @Override
    public int hashCode() {
        return rawKey.hashCode();
    }

    @Override
    public String toString() {
        return rawKey + " [" + getReferenceCount() + "]";
    }
}