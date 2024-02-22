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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.DefaultConfigurableOptionsFactory;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBOperationUtils;
import org.apache.flink.contrib.streaming.state.RocksDBResourceContainer;
import org.apache.flink.contrib.streaming.state.RocksDBSharedResources;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.async.BatchingComponent;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.apache.flink.state.forst.ForStOptions.ForStMode;

import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;

import org.rocksdb.DBOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.apache.flink.state.forst.ForStOptions.FOR_ST_READ_AHEAD_FOR_COMPACTION;

@PublicEvolving
public class ForStStateBackend extends EmbeddedRocksDBStateBackend {

    private static final Logger LOG = LoggerFactory.getLogger(ForStStateBackend.class);

    private ForStMode forStMode;

    private int ioParallelism;

    private String workingDir;

    public ForStStateBackend() {
        super();
    }

    public ForStStateBackend(
            ForStStateBackend original, ReadableConfig config, ClassLoader classLoader) {
        super(original, config, classLoader);
        boolean init = original.forStMode != null;
        this.forStMode = init ? original.forStMode : config.get(ForStOptions.FOR_ST_MODE);
        this.workingDir = init ? original.workingDir : config.get(ForStOptions.FOR_ST_WORKING_DIR);
        this.ioParallelism = init ? original.ioParallelism : config.get(ForStOptions.FOR_ST_IO_PARALLELISM);
        ForStFlinkFileSystem.configureCacheTtl(
                config.get(ForStOptions.FOR_ST_FS_CACHE_LIVE_MILLS),
                config.get(ForStOptions.FOR_ST_FS_CACHE_TIMEOUT_MILLS),
                config.get(ForStOptions.FOR_ST_FS_CACHE_CAPACITY));
        ForStFlinkFileSystem.configureBlockBasedCache(
                config.get(ForStOptions.FOR_ST_BLOCK_CACHE_SIZE));
        LOG.info("Create ForStStateBackend with remoteRocksDBMode {}, workingDir {}, ioParallelism {}",
                forStMode, workingDir, ioParallelism);
        setRocksDBOptions(new DefaultConfigurableOptionsFactory() {
            @Override
            public DBOptions createDBOptions(DBOptions dbOptions, Collection<AutoCloseable> collection) {
                super.createDBOptions(dbOptions, collection);

                long readAhead = config.get(FOR_ST_READ_AHEAD_FOR_COMPACTION);
                dbOptions.setCompactionReadaheadSize(readAhead);
                LOG.info("Compaction read ahead size set to {}.", readAhead);
                dbOptions.setUseDirectIoForFlushAndCompaction(true);
                dbOptions.setUseDirectReads(true);
                return dbOptions;
            }
        });
    }

    @Override
    public ForStStateBackend configure(ReadableConfig config, ClassLoader classLoader) {
        return new ForStStateBackend(this, config, classLoader);
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry,
            double managedMemoryFraction,
            BatchingComponent<?, K> batchingComponent) throws IOException {

        // first, make sure that the RocksDB JNI library is loaded
        // we do this explicitly here to have better error handling
        String tempDir = env.getTaskManagerInfo().getTmpWorkingDirectory().getAbsolutePath();
        ensureRocksDBIsLoaded(tempDir);

        Path tempCachePath =
                new Path(tempDir,
                        "rocksdb_dfs_cache");

        ForStFlinkFileSystem.configureCacheBase(tempCachePath);
        ForStFlinkFileSystem.configureMetrics(metricGroup);

        // replace all characters that are not legal for filenames with underscore
        String fileCompatibleIdentifier = operatorIdentifier.replaceAll("[^a-zA-Z0-9\\-]", "_");

        lazyInitializeForJob(env, fileCompatibleIdentifier);

        File instanceBasePath =
                new File(
                        getNextStoragePath(),
                        "job_"
                                + jobId
                                + "_op_"
                                + fileCompatibleIdentifier
                                + "_uuid_"
                                + UUID.randomUUID());

        LocalRecoveryConfig localRecoveryConfig =
                env.getTaskStateManager().createLocalRecoveryConfig();

        final OpaqueMemoryResource<RocksDBSharedResources> sharedResources =
                RocksDBOperationUtils.allocateSharedCachesIfConfigured(
                        memoryConfiguration, env, managedMemoryFraction, LOG, rocksDBMemoryFactory);
        if (sharedResources != null) {
            LOG.info("Obtained shared RocksDB cache of size {} bytes", sharedResources.getSize());
        }
        final RocksDBResourceContainer resourceContainer =
                createOptionsAndResourceContainer(
                        sharedResources,
                        instanceBasePath,
                        nativeMetricOptions.isStatisticsEnabled());

        ExecutionConfig executionConfig = env.getExecutionConfig();
        StreamCompressionDecorator keyGroupCompressionDecorator =
                getCompressionDecorator(executionConfig);

        LatencyTrackingStateConfig latencyTrackingStateConfig =
                latencyTrackingConfigBuilder.setMetricGroup(metricGroup).build();
        ForStKeyedStateBackendBuilder<K> builder =
                (ForStKeyedStateBackendBuilder<K>)
                        new ForStKeyedStateBackendBuilder<>(
                                forStMode,
                                workingDir,
                                ioParallelism,
                                operatorIdentifier,
                                env.getUserCodeClassLoader().asClassLoader(),
                                instanceBasePath,
                                resourceContainer,
                                stateName -> resourceContainer.getColumnOptions(),
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
                                cancelStreamRegistry,
                                batchingComponent)
                        .setEnableIncrementalCheckpointing(isIncrementalCheckpointsEnabled())
                        .setNumberOfTransferingThreads(getNumberOfTransferThreads())
                        .setNativeMetricOptions(
                                resourceContainer.getMemoryWatcherOptions(nativeMetricOptions))
                        .setWriteBatchSize(getWriteBatchSize())
                        .setOverlapFractionThreshold(getOverlapFractionThreshold());
        return builder.build();
    }

    protected void lazyInitializeForJob(
            Environment env, @SuppressWarnings("unused") String operatorIdentifier) {

        if (isInitialized) {
            return;
        }

        this.jobId = env.getJobID();

        // initialize the paths where the local RocksDB files should be stored
        if (localRocksDbDirectories == null) {
            initializedDbBasePaths = new File[] {env.getTaskManagerInfo().getTmpWorkingDirectory()};
        } else {
            List<File> dirs = new ArrayList<>(localRocksDbDirectories.length);

            Collections.addAll(dirs, localRocksDbDirectories);
            initializedDbBasePaths = dirs.toArray(new File[0]);
        }

        nextDirectory = new Random().nextInt(initializedDbBasePaths.length);
        isInitialized = true;
    }
}
