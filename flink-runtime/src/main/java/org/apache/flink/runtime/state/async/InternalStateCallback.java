package org.apache.flink.runtime.state.async;

import org.apache.flink.api.common.state.async.Callback;
import org.apache.flink.runtime.state.heap.InternalKeyContext;

public class InternalStateCallback<K, V> {
    private final K currentKey;
    private final Callback<V> userCallBack;

    private final InternalKeyContext<K> internalKeyContext;

    InternalStateCallback(K currentKey,
                          Callback<V> userCallBack,
                          InternalKeyContext<K> internalKeyContext) {
        this.currentKey = currentKey;
        this.userCallBack = userCallBack;
        this.internalKeyContext = internalKeyContext;
    }


    public void accept(V value) throws Exception {
        internalKeyContext.setCurrentKey(currentKey, true);
        userCallBack.accept(value);
    }
}
