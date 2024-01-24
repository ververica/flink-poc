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
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.async.AsyncState;
import org.apache.flink.api.common.state.async.AsyncStateDescriptor;
import org.apache.flink.api.common.state.async.AsyncValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricMonitor;
import org.apache.flink.contrib.streaming.state.RocksDBResourceContainer;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.contrib.streaming.state.snapshot.RocksDBSnapshotStrategyBase;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.async.BatchCacheStateConfig;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.remote.rocksdb.RemoteRocksDBOptions.RemoteRocksDBMode;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.function.RunnableWithException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

public class RemoteRocksDBKeyedStateBackend<K> extends RocksDBKeyedStateBackend<K> {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteRocksDBKeyedStateBackend.class);

    private RemoteRocksDBMode remoteRocksDBMode;
    private String workingDir;

    private final BatchCacheStateConfig batchCacheStateConfig;

    private final BatchParallelIOExecutor<K> batchParallelIOExecutor;

    private final Consumer<RunnableWithException> registerCallBackFunc;

    private final Consumer<Integer> updateOngoingStateReq;

    public RemoteRocksDBKeyedStateBackend(
            RemoteRocksDBMode remoteRocksDBMode,
            String workingDir,
            boolean enableCacheLayer,
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
            Consumer<RunnableWithException> registerCallBackFunc,
            Consumer<Integer> updateOngoingStateReq) {
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
        this.batchCacheStateConfig = new BatchCacheStateConfig(enableCacheLayer);
        ExecutorService executor = Executors.newFixedThreadPool(ioParallelism);
        this.batchParallelIOExecutor = new BatchParallelIOExecutor<>(executor);
        this.registerCallBackFunc = registerCallBackFunc;
        this.updateOngoingStateReq = updateOngoingStateReq;
        LOG.info("Create RemoteRocksDBKeyedStateBackend: remoteRocksDBMode {}, workingDir {}, enableCacheLayer {}, ioParallelism {}",
                remoteRocksDBMode, workingDir, enableCacheLayer, ioParallelism);
    }
    
    RocksDB getDB() {
        return db;
    }

    @Override
    public void setCurrentKey(K newKey) {
        setKeyContext(newKey);
    }

    @Override
    public boolean isSupportAsync() {
        return true;
    }

    @Override
    public Consumer<RunnableWithException> getRegisterCallBackFunc() {
        return registerCallBackFunc;
    }

    @Override
    public Consumer<Integer> updateOngoingStateReqFunc() {
        return updateOngoingStateReq;
    }

    @Override
    public BatchCacheStateConfig getBatchCacheStateConfig() {
        return batchCacheStateConfig;
    }

    @Override
    public <N, SV, SEV, S extends AsyncState, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull AsyncStateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws Exception {

        State state = createOrUpdateInternalState(namespaceSerializer, convertStateDescriptor(stateDesc), snapshotTransformFactory, allowFutureMetadataUpdates);
        // TODO: Implement
        // AsyncValueState v = Factory.wrap(ValueState)
        // AsyncValueState v1 = Batching.wrap(v)
        return null;
    }

    private <S extends State, AS extends AsyncState, SV> StateDescriptor<S, SV> convertStateDescriptor(@Nonnull AsyncStateDescriptor<AS, SV> stateDesc) {
        if (stateDesc instanceof AsyncValueStateDescriptor) {
            return (StateDescriptor<S, SV>) new ValueStateDescriptor<>(stateDesc.getName(), stateDesc.getSerializer());
        }
        return null;
    }

    @Override
    protected void cleanInstanceBasePath() {
        if (remoteRocksDBMode == RemoteRocksDBMode.LOCAL) {
            super.cleanInstanceBasePath();
        }
        //TODO
    }

    public BatchParallelIOExecutor<K> getBatchParallelIOExecutor() {
        return batchParallelIOExecutor;
    }

    @Override
    public void close() throws IOException {
        batchParallelIOExecutor.close();
        super.close();
    }
}
