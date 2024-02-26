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

package org.apache.flink.state.forst.async;

import org.apache.flink.api.common.state.async.AsyncState;
import org.apache.flink.api.common.state.async.AsyncStateDescriptor;
import org.apache.flink.api.common.state.async.AsyncValueState;
import org.apache.flink.api.common.state.async.StateFuture;
import org.apache.flink.api.common.state.async.StateUncheckedIOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.async.StateFutureImpl;
import org.apache.flink.runtime.state.async.StateRequest;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.state.forst.ForStKeyedStateBackend;
import org.apache.flink.state.forst.internal.ForStValueState;

import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;

public class AsyncForStValueState<R, K, N, V>
        extends AbstractAsyncForStState<R, K, N, V> implements AsyncValueState<V> {

    private final ForStValueState<K, N, V> syncValueState;

    AsyncForStValueState(
            ForStKeyedStateBackend<R, K> backend,
            InternalKeyContext<K> keyContext,
            ColumnFamilyHandle columnFamily,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue) {
        super(backend, keyContext, keySerializer, namespaceSerializer, valueSerializer);
        this.syncValueState = new ForStValueState<>(backend, columnFamily, keySerializer,
                namespaceSerializer, valueSerializer, defaultValue);
        stateExecutor.registerState(syncValueState);
    }

    @Override
    public StateFuture<V> value() {
        StateFutureImpl<R, K, V> future = batchingComponent.newStateFuture(
                keyContext.getCurrentKey(), keyContext.getCurrentRecordContext(), keyContext);
        StateRequest<ForStValueState<K, N, V>, K, Void, V> valueGet =
                StateRequest.ofValueGet(syncValueState, keyContext.getCurrentKey(), future);
        try {
            batchingComponent.processStateRequest(valueGet, keyContext.getCurrentRecordContext());
            return future;
        } catch (IOException e) {
            throw new StateUncheckedIOException(e);
        }
    }

    @Override
    public StateFuture<Void> update(V value) {
        StateFutureImpl<R, K, Void> future = batchingComponent.newStateFuture(
                keyContext.getCurrentKey(), keyContext.getCurrentRecordContext(), keyContext);
        StateRequest<ForStValueState<K, N, V>, K, V, Void> valuePut =
                StateRequest.ofValuePut(syncValueState, keyContext.getCurrentKey(), value, future);
        try {
            batchingComponent.processStateRequest(valuePut, keyContext.getCurrentRecordContext());
            return future;
        } catch (IOException e) {
            throw new StateUncheckedIOException(e);
        }
    }

    public static <R, K, N, SV, S extends AsyncState, IS extends S> IS create(
            AsyncStateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            ForStKeyedStateBackend<R, K> backend) {
        return (IS)
                new AsyncForStValueState<>(
                        backend,
                        backend.getKeyContext(),
                        registerResult.f0,
                        backend.getKeySerializer(),
                        registerResult.f1.getNamespaceSerializer(),
                        registerResult.f1.getStateSerializer(),
                        stateDesc.getDefaultValue());
    }
}
