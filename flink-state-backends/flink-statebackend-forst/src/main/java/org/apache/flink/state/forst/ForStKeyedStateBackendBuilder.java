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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackendBuilder;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricMonitor;
import org.apache.flink.contrib.streaming.state.RocksDBOperationUtils;
import org.apache.flink.contrib.streaming.state.RocksDBPriorityQueueConfig;
import org.apache.flink.contrib.streaming.state.RocksDBResourceContainer;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.contrib.streaming.state.restore.RocksDBIncrementalRestoreOperation;
import org.apache.flink.contrib.streaming.state.restore.RocksDBNoneRestoreOperation;
import org.apache.flink.contrib.streaming.state.restore.RocksDBRestoreOperation;
import org.apache.flink.contrib.streaming.state.restore.RocksDBRestoreResult;
import org.apache.flink.contrib.streaming.state.snapshot.RocksDBSnapshotStrategyBase;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.async.BatchingComponent;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.apache.flink.state.forst.snapshot.ForStIncrementalSnapshotStrategy;
import org.apache.flink.state.forst.snapshot.ForStStateUploader;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlinkEnv;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;

/**
 * Builder class for {@link ForStKeyedStateBackend} which handles all necessary initializations
 * and clean ups.
 *
 * @param <K> The data type that the key serializer serializes.
 */
public class ForStKeyedStateBackendBuilder<K> extends RocksDBKeyedStateBackendBuilder<K> {

    private static final Logger LOG = LoggerFactory.getLogger(ForStKeyedStateBackendBuilder.class);

    private ForStOptions.ForStMode forStMode;

    private String workingDir;

    private final int ioParallelism;

    private final BatchingComponent<?, K> batchingComponent;

    public ForStKeyedStateBackendBuilder(
            ForStOptions.ForStMode forStMode,
            String workingDir,
            int ioParallelism,
            String operatorIdentifier,
            ClassLoader userCodeClassLoader,
            File instanceBasePath,
            RocksDBResourceContainer optionsContainer,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            ExecutionConfig executionConfig,
            LocalRecoveryConfig localRecoveryConfig,
            RocksDBPriorityQueueConfig priorityQueueConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            CloseableRegistry cancelStreamRegistry,
            BatchingComponent<?, K> batchingComponent) {
        super(
                operatorIdentifier,
                userCodeClassLoader,
                instanceBasePath,
                optionsContainer,
                columnFamilyOptionsFactory,
                kvStateRegistry,
                keySerializer,
                numberOfKeyGroups,
                keyGroupRange,
                executionConfig,
                localRecoveryConfig,
                priorityQueueConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                metricGroup,
                stateHandles,
                keyGroupCompressionDecorator,
                cancelStreamRegistry);
        this.forStMode = forStMode;
        this.workingDir = workingDir;
        this.ioParallelism = ioParallelism;
        this.batchingComponent = batchingComponent;
    }

    @Override
    public ForStKeyedStateBackend<?, K> build() throws BackendBuildingException {
        RocksDBWriteBatchWrapper writeBatchWrapper = null;
        ColumnFamilyHandle defaultColumnFamilyHandle = null;
        RocksDBNativeMetricMonitor nativeMetricMonitor = null;
        CloseableRegistry cancelStreamRegistryForBackend = new CloseableRegistry();
        LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation =
                new LinkedHashMap<>();
        LinkedHashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates =
                new LinkedHashMap<>();
        RocksDB db = null;
        RocksDBRestoreOperation restoreOperation = null;
        RocksDbTtlCompactFiltersManager ttlCompactFiltersManager =
                new RocksDbTtlCompactFiltersManager(ttlTimeProvider);

        ResourceGuard rocksDBResourceGuard = new ResourceGuard();
        RocksDBSnapshotStrategyBase<K, ?> checkpointStrategy = null;
        PriorityQueueSetFactory priorityQueueFactory;
        SerializedCompositeKeyBuilder<K> sharedRocksKeyBuilder;
        // Number of bytes required to prefix the key groups.
        int keyGroupPrefixBytes =
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                        numberOfKeyGroups);

        try {
            // Variables for snapshot strategy when incremental checkpoint is enabled
            UUID backendUID = UUID.randomUUID();
            SortedMap<Long, Collection<IncrementalKeyedStateHandle.HandleAndLocalPath>> materializedSstFiles = new TreeMap<>();
            long lastCompletedCheckpointId = -1L;

            prepareDirectories();
            restoreOperation =
                    getRocksDBRestoreOperation(
                            keyGroupPrefixBytes,
                            cancelStreamRegistry,
                            kvStateInformation,
                            registeredPQStates,
                            ttlCompactFiltersManager);
            RocksDBRestoreResult restoreResult = restoreOperation.restore();
            db = restoreResult.getDb();
            defaultColumnFamilyHandle = restoreResult.getDefaultColumnFamilyHandle();
            nativeMetricMonitor = restoreResult.getNativeMetricMonitor();
            if (restoreOperation instanceof RocksDBIncrementalRestoreOperation) {
                backendUID = restoreResult.getBackendUID();
                materializedSstFiles = restoreResult.getRestoredSstFiles();
                lastCompletedCheckpointId = restoreResult.getLastCompletedCheckpointId();
            }

            writeBatchWrapper =
                    new RocksDBWriteBatchWrapper(
                            db, optionsContainer.getWriteOptions(), writeBatchSize);

            // it is important that we only create the key builder after the restore, and not
            // before;
            // restore operations may reconfigure the key serializer, so accessing the key
            // serializer
            // only now we can be certain that the key serializer used in the builder is final.
            sharedRocksKeyBuilder =
                    new SerializedCompositeKeyBuilder<>(
                            keySerializerProvider.currentSchemaSerializer(),
                            keyGroupPrefixBytes,
                            32);
            // init snapshot strategy after db is assured to be initialized
            checkpointStrategy =
                    initializeSavepointAndCheckpointStrategies(
                            cancelStreamRegistryForBackend,
                            rocksDBResourceGuard,
                            kvStateInformation,
                            registeredPQStates,
                            keyGroupPrefixBytes,
                            db,
                            backendUID,
                            materializedSstFiles,
                            lastCompletedCheckpointId);
            // init priority queue factory
            priorityQueueFactory =
                    initPriorityQueueFactory(
                            keyGroupPrefixBytes,
                            kvStateInformation,
                            db,
                            writeBatchWrapper,
                            nativeMetricMonitor);
        } catch (Throwable e) {
            // Do clean up
            List<ColumnFamilyOptions> columnFamilyOptions =
                    new ArrayList<>(kvStateInformation.values().size());
            IOUtils.closeQuietly(cancelStreamRegistryForBackend);
            IOUtils.closeQuietly(writeBatchWrapper);
            RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(
                    columnFamilyOptions, defaultColumnFamilyHandle);
            IOUtils.closeQuietly(defaultColumnFamilyHandle);
            IOUtils.closeQuietly(nativeMetricMonitor);
            for (RocksDBKeyedStateBackend.RocksDbKvStateInfo kvStateInfo :
                    kvStateInformation.values()) {
                RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(
                        columnFamilyOptions, kvStateInfo.columnFamilyHandle);
                IOUtils.closeQuietly(kvStateInfo.columnFamilyHandle);
            }
            IOUtils.closeQuietly(db);
            // it's possible that db has been initialized but later restore steps failed
            IOUtils.closeQuietly(restoreOperation);
            IOUtils.closeAllQuietly(columnFamilyOptions);
            IOUtils.closeQuietly(optionsContainer);
            ttlCompactFiltersManager.disposeAndClearRegisteredCompactionFactories();
            kvStateInformation.clear();
            IOUtils.closeQuietly(checkpointStrategy);
            try {
                FileUtils.deleteDirectory(instanceBasePath);
            } catch (Exception ex) {
                logger.warn("Failed to delete base path for RocksDB: " + instanceBasePath, ex);
            }
            // Log and rethrow
            if (e instanceof BackendBuildingException) {
                throw (BackendBuildingException) e;
            } else {
                String errMsg = "Caught unexpected exception.";
                logger.error(errMsg, e);
                throw new BackendBuildingException(errMsg, e);
            }
        }
        InternalKeyContext<K> keyContext =
                new InternalKeyContextImpl<>(keyGroupRange, numberOfKeyGroups);
        logger.info("Finished building RocksDB keyed state-backend at {}.", instanceBasePath);
        return new ForStKeyedStateBackend<>(
                this.forStMode,
                this.workingDir,
                this.ioParallelism,
                this.userCodeClassLoader,
                this.instanceBasePath,
                this.optionsContainer,
                columnFamilyOptionsFactory,
                this.kvStateRegistry,
                this.keySerializerProvider.currentSchemaSerializer(),
                this.executionConfig,
                this.ttlTimeProvider,
                latencyTrackingStateConfig,
                db,
                kvStateInformation,
                registeredPQStates,
                keyGroupPrefixBytes,
                cancelStreamRegistryForBackend,
                this.keyGroupCompressionDecorator,
                rocksDBResourceGuard,
                checkpointStrategy,
                writeBatchWrapper,
                defaultColumnFamilyHandle,
                nativeMetricMonitor,
                sharedRocksKeyBuilder,
                priorityQueueFactory,
                ttlCompactFiltersManager,
                keyContext,
                writeBatchSize,
                batchingComponent);
    }

    @Override
    protected RocksDBRestoreOperation getRocksDBRestoreOperation(
            int keyGroupPrefixBytes,
            CloseableRegistry cancelStreamRegistry,
            LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
            LinkedHashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            RocksDbTtlCompactFiltersManager ttlCompactFiltersManager) {
        DBOptions dbOptions = optionsContainer.getDbOptions();
        if (forStMode == ForStOptions.ForStMode.REMOTE) {
            dbOptions.setCreateIfMissing(true)
                    .setEnv(new FlinkEnv(workingDir))
                    .setDbLogDir(".")
                    .setLogger(new org.rocksdb.Logger(dbOptions) {
                        @Override
                        protected void log(InfoLogLevel infoLogLevel, String message) {
                            LOG.info("RocksDB [{}]: {}", infoLogLevel, message);
                        }
                    });
        }
        if (restoreStateHandles.isEmpty()) {
            return new RocksDBNoneRestoreOperation<>(
                    kvStateInformation,
                    instanceRocksDBPath,
                    dbOptions,
                    columnFamilyOptionsFactory,
                    nativeMetricOptions,
                    metricGroup,
                    ttlCompactFiltersManager,
                    optionsContainer.getWriteBufferManagerCapacity());
        }
        throw new UnsupportedOperationException("Don't support restoring yet");
    }

    @Override
    protected void prepareDirectories() throws IOException {
        if (forStMode == ForStOptions.ForStMode.LOCAL) {
            super.prepareDirectories();
        }
        //TODO
    }

    private ForStIncrementalSnapshotStrategy<K> initializeSavepointAndCheckpointStrategies(
            CloseableRegistry cancelStreamRegistry,
            ResourceGuard rocksDBResourceGuard,
            LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
            LinkedHashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            int keyGroupPrefixBytes,
            RocksDB db,
            UUID backendUID,
            SortedMap<Long, Collection<IncrementalKeyedStateHandle.HandleAndLocalPath>> materializedSstFiles,
            long lastCompletedCheckpointId) {
        ForStIncrementalSnapshotStrategy<K> checkpointSnapshotStrategy;
        ForStStateUploader stateUploader = new ForStStateUploader(
                (forStMode == ForStOptions.ForStMode.REMOTE ?
                        new Path(workingDir, instanceBasePath.getPath()) : new Path(instanceBasePath.toURI())),
                numberOfTransferingThreads);
        if (enableIncrementalCheckpointing) {
            checkpointSnapshotStrategy =
                    new ForStIncrementalSnapshotStrategy<>(
                            db,
                            rocksDBResourceGuard,
                            keySerializerProvider.currentSchemaSerializer(),
                            kvStateInformation,
                            keyGroupRange,
                            stateUploader,
                            instanceBasePath,
                            backendUID,
                            lastCompletedCheckpointId);
        } else {
            throw new UnsupportedOperationException("Full checkpointing strategy hasn't been "
                    + "supported for RemoteRocksdbKeyedStateBackend");
        }
        return checkpointSnapshotStrategy;
    }
}
