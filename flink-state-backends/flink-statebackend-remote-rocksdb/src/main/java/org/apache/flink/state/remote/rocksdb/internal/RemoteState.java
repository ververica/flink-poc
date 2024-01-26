package org.apache.flink.state.remote.rocksdb.internal;

import org.apache.flink.annotation.Internal;

@Internal
public interface RemoteState {

    void clear();
}
