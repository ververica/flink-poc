/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.remote.rocksdb;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.async.AsyncState;
import org.apache.flink.api.common.state.async.AsyncStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricMonitor;
import org.apache.flink.contrib.streaming.state.RocksDBResourceContainer;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.contrib.streaming.state.snapshot.RocksDBSnapshotStrategyBase;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.async.BatchingComponent;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.remote.rocksdb.RemoteRocksDBOptions.RemoteRocksDBMode;
import org.apache.flink.state.remote.rocksdb.async.AsyncRocksdbValueState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RemoteRocksDBKeyedStateBackend<R, K> extends RocksDBKeyedStateBackend<K> {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteRocksDBKeyedStateBackend.class);

    protected final Map<String, AsyncState> createdAsyncKVStates;

    private RemoteRocksDBMode remoteRocksDBMode;
    private String workingDir;

    private final StateExecutorService<K> stateExecutorService;

    private final BatchingComponent<R, K> batchingComponent;

    public RemoteRocksDBKeyedStateBackend(
            RemoteRocksDBMode remoteRocksDBMode,
            String workingDir,
            int ioParallelism,
            ClassLoader userCodeClassLoader,
            File instanceBasePath,
            RocksDBResourceContainer optionsContainer,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            RocksDB db,
            LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            int keyGroupPrefixBytes,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            ResourceGuard rocksDBResourceGuard,
            RocksDBSnapshotStrategyBase<K, ?> checkpointSnapshotStrategy,
            RocksDBWriteBatchWrapper writeBatchWrapper,
            ColumnFamilyHandle defaultColumnFamilyHandle,
            RocksDBNativeMetricMonitor nativeMetricMonitor,
            SerializedCompositeKeyBuilder<K> sharedRocksKeyBuilder,
            PriorityQueueSetFactory priorityQueueFactory,
            RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            InternalKeyContext<K> keyContext,
            long writeBatchSize,
            BatchingComponent<R, K> batchingComponent) {
        super(
                userCodeClassLoader,
                instanceBasePath,
                optionsContainer,
                columnFamilyOptionsFactory,
                kvStateRegistry,
                keySerializer,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                db,
                kvStateInformation,
                registeredPQStates,
                keyGroupPrefixBytes,
                cancelStreamRegistry,
                keyGroupCompressionDecorator,
                rocksDBResourceGuard,
                checkpointSnapshotStrategy,
                writeBatchWrapper,
                defaultColumnFamilyHandle,
                nativeMetricMonitor,
                sharedRocksKeyBuilder,
                priorityQueueFactory,
                ttlCompactFiltersManager,
                keyContext,
                writeBatchSize);
        this.remoteRocksDBMode = remoteRocksDBMode;
        this.workingDir = workingDir;
        this.createdAsyncKVStates = new HashMap<>();
        this.stateExecutorService = new StateExecutorService<>(ioParallelism);
        this.batchingComponent = batchingComponent;
        batchingComponent.setStateExecutor(stateExecutorService);
        LOG.info("Create RemoteRocksDBKeyedStateBackend: remoteRocksDBMode {}, workingDir {}, ioParallelism {}",
                remoteRocksDBMode, workingDir, ioParallelism);
    }
    
    public RocksDB getDB() {
        return db;
    }

    @Override
    public void setCurrentKey(K newKey) {
        setKeyContext(newKey);
    }

    public StateExecutorService<K> getStateExecutorService() {
        return stateExecutorService;
    }

    public BatchingComponent<R, K> getBatchingComponent() {
        return batchingComponent;
    }

    @Override
    public <N, SV, SEV, S extends AsyncState, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull AsyncStateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws Exception {
        Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> registerResult =
                tryRegisterKvStateInformation(
                        stateDesc,
                        namespaceSerializer,
                        snapshotTransformFactory,
                        allowFutureMetadataUpdates);
        if (!allowFutureMetadataUpdates) {
            // Config compact filter only when no future metadata updates
            ttlCompactFiltersManager.configCompactFilter(
                    stateDesc, registerResult.f1.getStateSerializer());
        }

        return createState(stateDesc, registerResult);
    }

    private static final Map<StateDescriptor.Type, StateCreateFactory> STATE_CREATE_FACTORIES =
            Stream.of(
                            Tuple2.of(
                                    StateDescriptor.Type.VALUE,
                                    (StateCreateFactory) AsyncRocksdbValueState::create))
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    private <N, SV, S extends AsyncState, IS extends S> IS createState(
            AsyncStateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult)
            throws Exception {
        @SuppressWarnings("unchecked")
        IS createdState = (IS) createdAsyncKVStates.get(stateDesc.getName());
        if (createdState == null) {
            StateCreateFactory stateCreateFactory = STATE_CREATE_FACTORIES.get(stateDesc.getType());
            if (stateCreateFactory == null) {
                throw new FlinkRuntimeException(stateNotSupportedMessage(stateDesc));
            }
            createdState =
                    stateCreateFactory.createState(
                            stateDesc, registerResult, RemoteRocksDBKeyedStateBackend.this);
        } else {
            throw new UnsupportedOperationException("Don't support updating yet");
        }

        createdAsyncKVStates.put(stateDesc.getName(), createdState);
        return createdState;
    }

    private  <S extends AsyncState, SV> String stateNotSupportedMessage(
            AsyncStateDescriptor<S, SV> stateDesc) {
        return String.format(
                "State %s is not supported by %s", stateDesc.getClass(), this.getClass());
    }

    @Override
    protected void cleanInstanceBasePath() {
        if (remoteRocksDBMode == RemoteRocksDBMode.LOCAL) {
            super.cleanInstanceBasePath();
        }
        //TODO
    }

    protected interface StateCreateFactory {
        <R, K, N, SV, S extends AsyncState, IS extends S> IS createState(
                AsyncStateDescriptor<S, SV> stateDesc,
                Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                        registerResult,
                RemoteRocksDBKeyedStateBackend<R, K> backend)
                throws Exception;
    }

    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            final long checkpointId,
            final long timestamp,
            @Nonnull final CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {
        batchingComponent.drainAllInFlightDataBeforeStateSnapshot();
        return super.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
    }

}
