package org.apache.flink.state.remote.rocksdb.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.io.IOException;
import java.util.List;

@Internal
public interface RemoteValueState<K, N, V> extends RemoteState {

    V get(K key) throws IOException;

    List<V> multiGet(List<K> keys) throws IOException;

    void put(K key, V value) throws IOException;


}
