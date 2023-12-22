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

package org.apache.flink.state.remote.rocksdb;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBOperationUtils;
import org.apache.flink.contrib.streaming.state.RocksDBResourceContainer;
import org.apache.flink.contrib.streaming.state.RocksDBSharedResources;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.apache.flink.state.remote.rocksdb.RemoteRocksDBOptions.RemoteRocksDBMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.UUID;

@PublicEvolving
public class RemoteRocksDBStateBackend extends EmbeddedRocksDBStateBackend {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteRocksDBStateBackend.class);

    private RemoteRocksDBMode remoteRocksDBMode;

    private boolean enableCacheLayer;

    private int ioParallelism;

    private String workingDir;

    public RemoteRocksDBStateBackend() {
        super();
    }

    public RemoteRocksDBStateBackend(
            EmbeddedRocksDBStateBackend original, ReadableConfig config, ClassLoader classLoader) {
        super(original, config, classLoader);
        this.remoteRocksDBMode = config.get(RemoteRocksDBOptions.REMOTE_ROCKSDB_MODE);
        this.enableCacheLayer = config.get(RemoteRocksDBOptions.REMOTE_ROCKSDB_ENABLE_CACHE_LAYER);
        this.workingDir = config.get(RemoteRocksDBOptions.REMOTE_ROCKSDB_WORKING_DIR);
        this.ioParallelism = config.get(RemoteRocksDBOptions.REMOTE_ROCKSDB_IO_PARALLELISM);
        LOG.info("Create RemoteRocksDBStateBackend with remoteRocksDBMode {}, enableCacheLayer {}, workingDir {}",
                remoteRocksDBMode, enableCacheLayer, workingDir);
    }

    @Override
    public RemoteRocksDBStateBackend configure(ReadableConfig config, ClassLoader classLoader) {
        return new RemoteRocksDBStateBackend(this, config, classLoader);
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
            double managedMemoryFraction) throws IOException {

        // first, make sure that the RocksDB JNI library is loaded
        // we do this explicitly here to have better error handling
        String tempDir = env.getTaskManagerInfo().getTmpWorkingDirectory().getAbsolutePath();
        ensureRocksDBIsLoaded(tempDir);

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
        RemoteRocksDBKeyedStateBackendBuilder<K> builder =
                (RemoteRocksDBKeyedStateBackendBuilder<K>)
                        new RemoteRocksDBKeyedStateBackendBuilder<>(
                                remoteRocksDBMode,
                                workingDir,
                                enableCacheLayer,
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
                                cancelStreamRegistry)
                        .setEnableIncrementalCheckpointing(isIncrementalCheckpointsEnabled())
                        .setNumberOfTransferingThreads(getNumberOfTransferThreads())
                        .setNativeMetricOptions(
                                resourceContainer.getMemoryWatcherOptions(nativeMetricOptions))
                        .setWriteBatchSize(getWriteBatchSize())
                        .setOverlapFractionThreshold(getOverlapFractionThreshold());
        return builder.build();
    }
}
