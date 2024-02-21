/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.remote.rocksdb.async;

import org.apache.flink.api.common.state.async.AsyncState;
import org.apache.flink.api.common.state.async.StateFuture;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.async.BatchingComponent;
import org.apache.flink.runtime.state.async.InternalAsyncState;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.state.remote.rocksdb.RemoteRocksDBKeyedStateBackend;
import org.apache.flink.state.remote.rocksdb.StateExecutorService;

public class AbstractAsyncRocksdbState<R, K, N, V> implements AsyncState, InternalAsyncState<K, N, V> {

    protected final BatchingComponent<R, K> batchingComponent;
    
    protected final StateExecutorService<K> stateExecutor;

    protected final InternalKeyContext<K> keyContext;

    protected final TypeSerializer<K> keySerializer;
    protected final TypeSerializer<N> namespaceSerializer;
    protected final TypeSerializer<V> valueSerializer;

    AbstractAsyncRocksdbState(
            RemoteRocksDBKeyedStateBackend<R, K> backend,
            InternalKeyContext<K> keyContext,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer) {
        this.batchingComponent = backend.getBatchingComponent();
        this.stateExecutor = backend.getStateExecutorService();
        this.keyContext = keyContext;
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public StateFuture<Void> clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public void setCurrentNamespace(Object namespace) {
        //throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer) throws Exception {
        throw new UnsupportedOperationException();
    }
}
