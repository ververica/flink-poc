package org.apache.flink.state.remote.rocksdb.snapshot;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateDataTransfer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.CheckedSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RemoteRocksdbStateUploader extends RocksDBStateDataTransfer {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteRocksdbStateUploader.class);

    private final Path remoteDBBasePath;

    public RemoteRocksdbStateUploader(Path remoteDBBasePath, int numberOfSnapshottingThreads) {
        super(numberOfSnapshottingThreads);
        this.remoteDBBasePath = remoteDBBasePath;
    }

    public Map<String, StreamStateHandle> uploadFilesToCheckpointDir(
            @Nonnull List<String> files,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry)
            throws Exception {

        List<CompletableFuture<Tuple2<String, StreamStateHandle>>> futures = files.stream()
                .map(childPath ->
                        Tuple2.of(childPath, new Path(remoteDBBasePath, "db/" + childPath)))
                .map(path ->
                        CompletableFuture.supplyAsync(
                                CheckedSupplier.unchecked(
                                        () -> Tuple2.of(path.f0, uploadLocalFileToCheckpointFs(
                                                path.f1,
                                                checkpointStreamFactory,
                                                stateScope,
                                                closeableRegistry,
                                                tmpResourcesRegistry))
                                                ),
                                executorService))
                .collect(Collectors.toList());

        FutureUtils.waitForAll(futures).get();
        Map<String, StreamStateHandle> handles = new HashMap<>();
        for (CompletableFuture<Tuple2<String, StreamStateHandle>> future : futures) {
            Tuple2<String, StreamStateHandle> result = future.get();
            handles.put(result.f0, result.f1);
        }
        return handles;
    }

    private StreamStateHandle uploadLocalFileToCheckpointFs(
            Path filePath,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry)
            throws IOException {

        final StreamStateHandle result;
        FileSystem sourceFileFs = FileSystem.get(filePath.toUri());
        long fileLen = sourceFileFs.getFileStatus(filePath).getLen();
        FileStateHandle sourceHandle = new FileStateHandle(filePath, fileLen);
        //Try fast copy
        if (checkpointStreamFactory.canFastDuplicate(sourceHandle, stateScope)) {
            List<StreamStateHandle> destStateHandle =
                    checkpointStreamFactory.duplicate(Collections.singletonList(sourceHandle), stateScope);
            Preconditions.checkState(destStateHandle.size() == 1);
            result = destStateHandle.get(0);
            tmpResourcesRegistry.registerCloseable(
                    () -> StateUtil.discardStateObjectQuietly(result));
            LOG.info("Upload file {} to checkpoint dir using fast-copy.", filePath);
            return result;
        }


        InputStream inputStream = null;
        CheckpointStateOutputStream outputStream = null;

        try {
            final byte[] buffer = new byte[16 * 1024];
            inputStream = sourceFileFs.open(filePath);
            closeableRegistry.registerCloseable(inputStream);
            outputStream = checkpointStreamFactory.createCheckpointStateOutputStream(stateScope);
            closeableRegistry.registerCloseable(outputStream);

            while (true) {
                int numBytes = inputStream.read(buffer);
                if (numBytes == -1) {
                    break;
                }
                outputStream.write(buffer, 0, numBytes);
            }

            if (closeableRegistry.unregisterCloseable(outputStream)) {
                result = outputStream.closeAndGetHandle();
                outputStream = null;
            } else {
                result = null;
            }
            tmpResourcesRegistry.registerCloseable(
                    () -> StateUtil.discardStateObjectQuietly(result));
            LOG.info("Upload file {} to checkpoint dir using copy.", filePath);
            return result;
        } finally {
            if (closeableRegistry.unregisterCloseable(inputStream)) {
                IOUtils.closeQuietly(inputStream);
            }

            if (closeableRegistry.unregisterCloseable(outputStream)) {
                IOUtils.closeQuietly(outputStream);
            }
        }

    }
}
