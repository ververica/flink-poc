package org.apache.flink.state.remote.rocksdb.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;

@Internal
public interface RemoteRocksdbKVState<K, N, V> {

    TypeSerializer<K> getKeySerializer();

    TypeSerializer<N> getNamespaceSerializer();

    TypeSerializer<V> getValueSerializer();
}
