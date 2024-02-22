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

package org.apache.flink.state.forst.fs;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.state.forst.fs.cache.BlockBasedCache;
import org.apache.flink.state.forst.fs.cache.FileBasedCache;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

/**
 * RemoteRocksdbFlinkFileSystem, used to expose flink fileSystem interface to frocksdb.
 */
public class ForStFlinkFileSystem extends FileSystem {

    private static Path cacheBase = null;

    private static long cacheTtl = 0L;

    private static long cacheTimeout = 0L;

    private static long cacheCapacity = 0L;

    private static long blockCacheSize = 0L;

    private static MetricGroup metricGroup = null;

    private final FileSystem flinkFS;

    private final BlockBasedCache blockBasedCache;

    private final FileBasedCache fileBasedCache;

    private final ByteBufferReadableFSDataInputStream.Metrics cacheMetricsReporter = new ByteBufferReadableFSDataInputStream.Metrics() {
        @Override
        public void hit() {
            if (fileBasedCache != null) {
                fileBasedCache.reportHit();
            }
        }

        @Override
        public void miss() {
            if (fileBasedCache != null) {
                fileBasedCache.reportMiss();
            }
        }
    };

    public ForStFlinkFileSystem(FileSystem flinkFS, FileBasedCache fileBasedCache, BlockBasedCache blockBasedCache) {
        this.flinkFS = flinkFS;
        this.fileBasedCache = fileBasedCache;
        this.blockBasedCache = blockBasedCache;
    }

    public static void configureCacheBase(Path path) {
        cacheBase = path;
    }

    public static void configureCacheTtl(long ttl, long timeout, long capacity) {
        cacheTtl = ttl;
        cacheTimeout = timeout;
        cacheCapacity = capacity;
    }

    public static void configureBlockBasedCache(long size) {
        blockCacheSize = size;
    }

    public static void configureMetrics(MetricGroup group) {
        metricGroup = group;
    }

    public static FileSystem get(URI uri) throws IOException {
        return new ForStFlinkFileSystem(
                FileSystem.get(uri),
                (cacheBase == null || cacheTtl <= 0L) ? null : new FileBasedCache(new LocalFileSystem(), childCacheBase(cacheBase), cacheTtl, cacheTimeout, cacheCapacity, metricGroup.addGroup("fs_cache")),
                blockCacheSize <= 0L ? null : new BlockBasedCache(Integer.MAX_VALUE, blockCacheSize, metricGroup.addGroup("block_cache"))
        );
    }

    private static Path childCacheBase(Path base) {
        return new Path(base, UUID.randomUUID().toString());
    }

    @Override
    public Path getWorkingDirectory() {
        return flinkFS.getWorkingDirectory();
    }

    @Override
    public Path getHomeDirectory() {
        return flinkFS.getHomeDirectory();
    }

    @Override
    public URI getUri() {
        return flinkFS.getUri();
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return flinkFS.getFileStatus(f);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(
            FileStatus file,
            long start,
            long len) throws IOException {
        return flinkFS.getFileBlockLocations(file, start, len);
    }

    @Override
    public ByteBufferReadableFSDataInputStream open(Path f, int bufferSize) throws IOException {
        FSDataInputStream original = flinkFS.open(f, bufferSize);
        long fileSize = flinkFS.getFileStatus(f).getLen();
        return new ByteBufferReadableFSDataInputStream(f, original, f.getName().endsWith("sst") ? blockBasedCache : null,
                fileBasedCache == null ? null : fileBasedCache.open4Read(f), cacheMetricsReporter,
                () -> flinkFS.open(f, bufferSize),
                fileSize);
    }

    @Override
    public ByteBufferReadableFSDataInputStream open(Path f) throws IOException {
        FSDataInputStream original = flinkFS.open(f);
        long fileSize = flinkFS.getFileStatus(f).getLen();
        return new ByteBufferReadableFSDataInputStream(f, original, f.getName().endsWith("sst") ? blockBasedCache : null,
                fileBasedCache == null ? null : fileBasedCache.open4Read(f), cacheMetricsReporter,
                () -> flinkFS.open(f),
                fileSize);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        return flinkFS.listStatus(f);
    }

    @Override
    public boolean exists(final Path f) throws IOException {
        return flinkFS.exists(f);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return flinkFS.delete(f, recursive);
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        return flinkFS.mkdirs(f);
    }

    public ByteBufferWritableFSDataOutputStream create(Path f) throws IOException {
        return create(f, WriteMode.OVERWRITE);
    }

    @Override
    public ByteBufferWritableFSDataOutputStream create(Path f, WriteMode overwriteMode) throws IOException {
        FSDataOutputStream original = flinkFS.create(f, overwriteMode);
        return new ByteBufferWritableFSDataOutputStream(f, original, fileBasedCache == null ? null : fileBasedCache.open4Write(f));
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        // The rename is not atomic for RocksDB. Some FileSystems e.g. HDFS, OSS does not allow a
        // renaming if the target already exists. So, we delete the target before attempting the
        // rename.
        if (flinkFS.exists(dst)) {
            boolean deleted = flinkFS.delete(dst, false);
            if (!deleted) {
                throw new IOException("Fail to delete dst path: " + dst);
            }
        }
        return flinkFS.rename(src, dst);
    }

    @Override
    public boolean isDistributedFS() {
        return flinkFS.isDistributedFS();
    }

    @Override
    public FileSystemKind getKind() {
        return flinkFS.getKind();
    }
}
