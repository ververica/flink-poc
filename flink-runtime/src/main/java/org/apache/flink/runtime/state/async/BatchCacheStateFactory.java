/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.async;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.batch.InternalBatchValueState;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateFactory;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.SupplierWithException;

import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BatchCacheStateFactory<
        K, N, V, S extends State, IS extends InternalKvState<K, N, ?>> {

    private final InternalKvState<K, N, ?> kvState;

    private final StateDescriptor<S, V> stateDescriptor;

    private final BatchCacheStateConfig batchCacheStateConfig;

    private final Map<StateDescriptor.Type, SupplierWithException<IS, Exception>> stateFactories;

    protected final InternalKeyContext<K> keyContext;

    private final Consumer<RunnableWithException> registerCallBackFunc;

    private final Consumer<Integer> updateOngoingStateReq;

    private BatchCacheStateFactory(
            InternalKvState<K, N, ?> kvState,
            StateDescriptor<S, V> stateDescriptor,
            BatchCacheStateConfig batchCacheStateConfig,
            InternalKeyContext<K> keyContext,
            Consumer<RunnableWithException> registerCallBackFunc,
            Consumer<Integer> updateOngoingStateReq) {
        this.kvState = kvState;
        this.stateDescriptor = stateDescriptor;
        this.batchCacheStateConfig = batchCacheStateConfig;
        this.stateFactories = createStateFactories();
        this.keyContext = keyContext;
        this.registerCallBackFunc = registerCallBackFunc;
        this.updateOngoingStateReq = updateOngoingStateReq;
    }

    public static <K, N, V, S extends State>
    InternalKvState<K, N, ?> createStateAndWrapWithBatchCacheIfEnabled(
            InternalKvState<K, N, ?> kvState,
            StateDescriptor<S, V> stateDescriptor,
            BatchCacheStateConfig batchCacheStateConfig,
            InternalKeyContext<K> keyContext,
            Consumer<RunnableWithException> registerCallBackFunc,
            Consumer<Integer> updateOngoingStateReq)
            throws Exception {
        if (batchCacheStateConfig.isEnableCacheBatchData()) {
            AbstractBatchAsyncState<K, N, V, ?>  asyncState =
                    (AbstractBatchAsyncState<K, N, V, ?>) (new BatchCacheStateFactory<>
                            (kvState, stateDescriptor, batchCacheStateConfig, keyContext, registerCallBackFunc, updateOngoingStateReq)
                    .createState());
            //keyedStateBackend.registerCurrentKeysChangedListener(batchState);
            return asyncState;
        }
        return kvState;
    }

    private IS createState() throws Exception {
        SupplierWithException<IS, Exception> stateFactory =
                stateFactories.get(stateDescriptor.getType());
        if (stateFactory == null) {
            String message =
                    String.format(
                            "State %s is not supported by %s",
                            stateDescriptor.getClass(), LatencyTrackingStateFactory.class);
            throw new FlinkRuntimeException(message);
        }
        return stateFactory.get();
    }

    private Map<StateDescriptor.Type, SupplierWithException<IS, Exception>> createStateFactories() {
        return Stream.of(
                        Tuple2.of(
                                StateDescriptor.Type.VALUE,
                                (SupplierWithException<IS, Exception>) this::createValueState))
                .collect(Collectors.toMap(t -> t.f0, t -> t.f1));
    }

    @SuppressWarnings({"unchecked"})
    private IS createValueState() {
        return (IS)
                new BatchAsyncValueState<>(
                        (InternalBatchValueState<K, N, V>) kvState,
                        keyContext,
                        registerCallBackFunc, updateOngoingStateReq);
    }
}
