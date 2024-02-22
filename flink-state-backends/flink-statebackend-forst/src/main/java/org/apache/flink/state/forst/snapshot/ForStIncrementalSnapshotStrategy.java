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

package org.apache.flink.state.forst.snapshot;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.snapshot.RocksDBSnapshotStrategyBase;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SnapshotResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;

import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;

import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import org.apache.flink.util.IOUtils;

import org.apache.flink.util.Preconditions;

import org.apache.flink.util.ResourceGuard;

import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;

public class ForStIncrementalSnapshotStrategy<K>
    extends RocksDBSnapshotStrategyBase<K, ForStIncrementalSnapshotStrategy.RemoteRocksdbSnapshotResource>
        implements CheckpointListener, SnapshotStrategy<KeyedStateHandle,
        ForStIncrementalSnapshotStrategy.RemoteRocksdbSnapshotResource> {

    private static final Logger LOG = LoggerFactory.getLogger(ForStIncrementalSnapshotStrategy.class);

    private final RocksDB db;

    @Nonnull protected final TypeSerializer<K> keySerializer;

    /** Key/Value state meta info from the backend. */
    @Nonnull protected final LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation;

    @Nonnull private final SortedMap<Long, Map<String, StreamStateHandle>> uploadedSstFiles;

    private long lastCompletedCheckpointId;

    private final ForStStateUploader stateUploader;

    @Nonnull private final UUID backendUID;

    @Nonnull protected final KeyGroupRange keyGroupRange;

    public ForStIncrementalSnapshotStrategy(
            RocksDB db,
            ResourceGuard rocksDBResourceGuard,
            TypeSerializer<K> keySerializer,
            LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
            KeyGroupRange keyGroupRange,
            ForStStateUploader stateUploader,
            File instanceBasePath,
            UUID backendUID,
            long lastCompletedCheckpointId) {
        super("Remote rocksdb incremental snapshot",
                db,
                rocksDBResourceGuard,
                keySerializer,
                kvStateInformation,
                keyGroupRange,
                8,
                new LocalRecoveryConfig(null),
                instanceBasePath,
                backendUID);
        this.db = db;
        this.keySerializer = keySerializer;
        this.kvStateInformation = kvStateInformation;
        this.keyGroupRange = keyGroupRange;
        this.backendUID = backendUID;
        this.stateUploader = stateUploader;
        this.lastCompletedCheckpointId = lastCompletedCheckpointId;
        this.uploadedSstFiles = new TreeMap<>();
    }


    @Override
    public RemoteRocksdbSnapshotResource syncPrepareResources(long checkpointId) throws Exception {
        LOG.info("Trigger snapshot sync phase for checkpoint {}", checkpointId);
        final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                new ArrayList<>(kvStateInformation.size());
        for (Map.Entry<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> stateMetaInfoEntry :
                kvStateInformation.entrySet()) {
            stateMetaInfoSnapshots.add(stateMetaInfoEntry.getValue().metaInfo.snapshot());
        }
        Map<String, StreamStateHandle> previousSnapshot;
        synchronized (uploadedSstFiles) {
            previousSnapshot = uploadedSstFiles.get(lastCompletedCheckpointId);
        }

        db.disableFileDeletions();
        RocksDB.LiveFiles liveFiles = db.getLiveFiles(true);
        return new RemoteRocksdbSnapshotResource(
                liveFiles,
                previousSnapshot != null ? previousSnapshot : Collections.emptyMap(),
                stateMetaInfoSnapshots,
                () -> {
                    db.enableFileDeletions(false);
                    return null;
                });
    }

    @Override
    public void close() {
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            RemoteRocksdbSnapshotResource syncPartResource,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {

        if (syncPartResource.stateMetaInfoSnapshots.isEmpty()) {
            return registry -> SnapshotResult.empty();
        }

        final Map<String, StreamStateHandle> previousSnapshot;
        final CheckpointType.SharingFilesStrategy sharingFilesStrategy =
                checkpointOptions.getCheckpointType().getSharingFilesStrategy();
        switch (sharingFilesStrategy) {
            case FORWARD_BACKWARD:
                previousSnapshot = syncPartResource.previousSnapshot;
                break;
            case FORWARD:
            case NO_SHARING:
                previousSnapshot = Collections.emptyMap();
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported sharing files strategy: " + sharingFilesStrategy);
        }

        return new RemoteRocksdbIncrementalSnapshotOperation(
                checkpointId,
                syncPartResource.liveFiles.files,
                streamFactory,
                syncPartResource.stateMetaInfoSnapshots,
                previousSnapshot,
                sharingFilesStrategy);
    }

    @Override
    public void notifyCheckpointComplete(long completedCheckpointId) throws Exception {
        synchronized (uploadedSstFiles) {
            if (completedCheckpointId > lastCompletedCheckpointId
                    && uploadedSstFiles.containsKey(completedCheckpointId)) {
                uploadedSstFiles
                        .keySet()
                        .removeIf(checkpointId -> checkpointId < completedCheckpointId);
                lastCompletedCheckpointId = completedCheckpointId;
            }
        }
    }

    @Override
    public void notifyCheckpointAborted(long abortedCheckpointId) {
        synchronized (uploadedSstFiles) {
            uploadedSstFiles.keySet().remove(abortedCheckpointId);
        }
    }

    @Override
    protected PreviousSnapshot snapshotMetaData(
            long checkpointId,
            @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
        throw new UnsupportedOperationException();
    }

    public static class RemoteRocksdbSnapshotResource implements SnapshotResources {

        private final RocksDB.LiveFiles liveFiles;

        private final Map<String, StreamStateHandle> previousSnapshot;

        @Nonnull protected final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;
        private final Callable<Void> releaseSourceHook;

        RemoteRocksdbSnapshotResource(
                RocksDB.LiveFiles files,
                Map<String, StreamStateHandle> previousSnapshot,
                List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
                Callable<Void> releaseSourceHook) {
            this.liveFiles = files;
            this.previousSnapshot = previousSnapshot;
            this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
            this.releaseSourceHook = releaseSourceHook;
        }

        @Override
        public void release() {
            try {
                releaseSourceHook.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final class RemoteRocksdbIncrementalSnapshotOperation implements SnapshotResultSupplier<KeyedStateHandle> {

        @Nonnull
        private final List<String> currentLiveFiles;

        /** Stream factory that creates the output streams to DFS. */
        @Nonnull
        private final CheckpointStreamFactory checkpointStreamFactory;

        @Nonnull
        private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

        @Nonnull
        private final CloseableRegistry tmpResourcesRegistry;

        @Nonnull
        private final Map<String, StreamStateHandle> previousSnapshot;

        @Nonnull
        private final SnapshotType.SharingFilesStrategy sharingFilesStrategy;

        private final long checkpointId;

        RemoteRocksdbIncrementalSnapshotOperation(
                long checkpointId,
                @Nonnull List<String> currentLiveFiles,
                @Nonnull CheckpointStreamFactory checkpointStreamFactory,
                @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
                Map<String, StreamStateHandle> previousSnapshot,
                @Nonnull SnapshotType.SharingFilesStrategy sharingFilesStrategy) {
            this.checkpointId = checkpointId;
            this.currentLiveFiles = currentLiveFiles;
            this.checkpointStreamFactory = checkpointStreamFactory;
            this.tmpResourcesRegistry = new CloseableRegistry();
            this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
            this.previousSnapshot = previousSnapshot;
            this.sharingFilesStrategy = sharingFilesStrategy;
        }

        @Override
        public SnapshotResult<KeyedStateHandle> get(CloseableRegistry snapshotCloseableRegistry)
                throws Exception {
            LOG.info("Start Async part for checkpoint {}", checkpointId);

            boolean completed = false;
            SnapshotResult<StreamStateHandle> metaStateHandle = null;
            Map<String, StreamStateHandle> sstFiles = new HashMap<>();
            Map<String, StreamStateHandle> miscFiles = new HashMap<>();
            try {
                metaStateHandle =
                        materializeMetaData(
                                snapshotCloseableRegistry,
                                tmpResourcesRegistry,
                                stateMetaInfoSnapshots,
                                checkpointStreamFactory);
                Preconditions.checkNotNull(metaStateHandle);
                Preconditions.checkNotNull(metaStateHandle.getJobManagerOwnedSnapshot());
                long checkpointedSize = metaStateHandle.getStateSize();
                checkpointedSize +=
                        uploadSnapshotFiles(
                                sstFiles,
                                miscFiles,
                                snapshotCloseableRegistry,
                                tmpResourcesRegistry);

                List<IncrementalKeyedStateHandle.HandleAndLocalPath> sharedStateHandles =
                        sstFiles.entrySet().stream()
                                .map(entry -> IncrementalKeyedStateHandle.HandleAndLocalPath.of(entry.getValue(), entry.getKey()))
                                .collect(Collectors.toList());
                List<IncrementalKeyedStateHandle.HandleAndLocalPath> privateStateHandles =
                        miscFiles.entrySet().stream()
                                .map(entry -> IncrementalKeyedStateHandle.HandleAndLocalPath.of(entry.getValue(), entry.getKey()))
                                .collect(Collectors.toList());

                final IncrementalRemoteKeyedStateHandle jmIncrementalKeyedStateHandle =
                        new IncrementalRemoteKeyedStateHandle(
                                backendUID,
                                keyGroupRange,
                                checkpointId,
                                sharedStateHandles,
                                privateStateHandles,
                                metaStateHandle.getJobManagerOwnedSnapshot(),
                                checkpointedSize);

                final SnapshotResult<KeyedStateHandle> snapshotResult = SnapshotResult.of(jmIncrementalKeyedStateHandle);
                completed = true;
                return snapshotResult;
            } finally {
                if (!completed) {
                    try {
                        tmpResourcesRegistry.close();
                    } catch (Exception e) {
                        LOG.warn("Could not properly clean tmp resources.", e);
                    }
                }
            }
        }

        private SnapshotResult<StreamStateHandle> materializeMetaData(
                @Nonnull CloseableRegistry snapshotCloseableRegistry,
                @Nonnull CloseableRegistry tmpResourcesRegistry,
                @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
                @Nonnull CheckpointStreamFactory checkpointStreamFactory) throws Exception {
            CheckpointStreamWithResultProvider streamWithResultProvider = CheckpointStreamWithResultProvider.createSimpleStream(
                    CheckpointedStateScope.EXCLUSIVE, checkpointStreamFactory);
            snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);
            try {
                // no need for compression scheme support because sst-files are already compressed
                KeyedBackendSerializationProxy<K> serializationProxy =
                        new KeyedBackendSerializationProxy<>(
                                keySerializer, stateMetaInfoSnapshots, false);

                DataOutputView out =
                        new DataOutputViewStreamWrapper(
                                streamWithResultProvider.getCheckpointOutputStream());

                serializationProxy.write(out);

                if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
                    SnapshotResult<StreamStateHandle> result =
                            streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
                    streamWithResultProvider = null;
                    tmpResourcesRegistry.registerCloseable(
                            () -> StateUtil.discardStateObjectQuietly(result));
                    LOG.info("End materializeMetaData for checkpoint {}", checkpointId);
                    return result;
                } else {
                    throw new IOException("Stream already closed and cannot return a handle.");
                }
            } finally {
                if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
                    IOUtils.closeQuietly(streamWithResultProvider);
                }
            }
        }

        private long uploadSnapshotFiles(
                @Nonnull Map<String, StreamStateHandle> sstFiles,
                @Nonnull Map<String, StreamStateHandle> miscFiles,
                @Nonnull CloseableRegistry snapshotCloseableRegistry,
                @Nonnull CloseableRegistry tmpResourcesRegistry)
                throws Exception {
            if (currentLiveFiles.isEmpty()) {
                return 0;
            }
            List<String> needUploadSstFilePaths = new ArrayList<>();
            List<String> needUploadMiscFilePaths = new ArrayList<>();
            long uploadedSize = 0;
            for (String file : currentLiveFiles) {
                if (file.contains("MANIFEST")) {
                    //TODO handle MANIFEST file sync
                    continue;
                }
                if (file.endsWith(SST_FILE_SUFFIX)) {
                    if (previousSnapshot.containsKey(file)) {
                        StreamStateHandle previousHandle = previousSnapshot.get(file);
                        StreamStateHandle handle = new PlaceholderStreamStateHandle(
                                previousHandle.getStreamStateHandleID(),
                                previousHandle.getStateSize());
                        sstFiles.put(file, handle);
                        LOG.info("Reuse file Hande for file {}", file);
                    } else {
                        needUploadSstFilePaths.add(file);
                    }
                } else {
                    needUploadMiscFilePaths.add(file);
                }
            }

            final CheckpointedStateScope stateScope =
                    sharingFilesStrategy == SnapshotType.SharingFilesStrategy.NO_SHARING
                            ? CheckpointedStateScope.EXCLUSIVE
                            : CheckpointedStateScope.SHARED;
            Map<String, StreamStateHandle> sstFilesUploadResult =
                    stateUploader.uploadFilesToCheckpointDir(
                            needUploadSstFilePaths,
                            checkpointStreamFactory,
                            stateScope,
                            snapshotCloseableRegistry,
                            tmpResourcesRegistry);
            uploadedSize +=
                    sstFilesUploadResult.values().stream().mapToLong(e -> e.getStateSize()).sum();
            sstFiles.putAll(sstFilesUploadResult);

            Map<String, StreamStateHandle> miscFilesUploadResult =
                    stateUploader.uploadFilesToCheckpointDir(
                            needUploadMiscFilePaths,
                            checkpointStreamFactory,
                            stateScope,
                            snapshotCloseableRegistry,
                            tmpResourcesRegistry);
            uploadedSize +=
                    miscFilesUploadResult.values().stream().mapToLong(e -> e.getStateSize()).sum();
            miscFiles.putAll(miscFilesUploadResult);

            synchronized (uploadedSstFiles) {
                switch (sharingFilesStrategy) {
                    case FORWARD_BACKWARD:
                    case FORWARD:
                        uploadedSstFiles.put(
                                checkpointId, Collections.unmodifiableMap(sstFiles));
                        break;
                    case NO_SHARING:
                        break;
                    default:
                        // This is just a safety precaution. It is checked before creating the
                        // RocksDBIncrementalSnapshotOperation
                        throw new IllegalArgumentException(
                                "Unsupported sharing files strategy: " + sharingFilesStrategy);
                }
            }

            return uploadedSize;
        }
    }
}
