package org.apache.flink.runtime.state.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.async.StateFuture;

@Internal
public interface InternalStateFuture<V> extends StateFuture<V> {
    
    void complete(V value);

    RecordContext<?, ?> getCurrentRecordContext();
}
