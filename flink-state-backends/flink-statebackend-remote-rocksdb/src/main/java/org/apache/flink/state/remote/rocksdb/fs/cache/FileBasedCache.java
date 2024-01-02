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

package org.apache.flink.state.remote.rocksdb.fs.cache;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class FileBasedCache implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(FileBasedCache.class);
    private final FileSystem cacheFs;

    private final Path basePath;

    private final long cacheTtl;

    private final ConcurrentHashMap<Path, CacheEntry> cacheMap;

    private final ScheduledExecutorService timeTickService;


    public FileBasedCache(FileSystem flinkFs, Path basePath, long cacheTtl) {
        this.cacheFs = flinkFs;
        this.basePath = basePath;
        this.cacheTtl = cacheTtl;
        this.cacheMap = new ConcurrentHashMap<>();
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("CacheDeleteTickTimeService-%d").build();
        this.timeTickService = new ScheduledThreadPoolExecutor(1, threadFactory);
        LOG.info("Local cache initialized at {}.", basePath);
    }

    @Override
    public void close() throws IOException {
        timeTickService.shutdownNow();
    }

    void scheduleDelete(CacheEntry e) {
        timeTickService.schedule(() -> {
            e.release();
        }, cacheTtl, TimeUnit.MILLISECONDS);
    }

    public CachedDataInputStream open4Read(Path path) throws IOException {
        CacheEntry entry = cacheMap.get(path);
        if (entry != null) {
            return new CachedDataInputStream(entry);
        } else {
            return null;
        }
    }

    public CachedDataOutputStream open4Write(Path path) throws IOException {
        CacheEntry entry = new CacheEntry(path, getCachePath(path));
        cacheMap.put(path, entry);
        return new CachedDataOutputStream(entry);
    }

    Path getCachePath(Path fromOriginal) {
        return new Path(basePath, fromOriginal.getName());
    }

    class CacheEntry extends ReferenceCounted {

        private final Path originalPath;

        private final Path cachePath;

        private volatile FSDataInputStream fsDataInputStream;

        private volatile FSDataOutputStream fsDataOutputStream;

        private volatile boolean writing;

        CacheEntry(Path originalPath, Path cachePath) {
            super(1);
            this.originalPath = originalPath;
            this.cachePath = cachePath;
            this.writing = true;
        }

        FSDataInputStream open4Read() throws IOException {
            if (!writing && tryRetain() > 0) {
                if (fsDataInputStream == null) {
                    fsDataInputStream = cacheFs.open(cachePath);
                }
                return fsDataInputStream;
            }
            return null;
        }

        FSDataOutputStream open4Write() throws IOException {
            if (writing && tryRetain() > 0) {
                if (fsDataOutputStream == null) {
                    fsDataOutputStream = cacheFs.create(cachePath, FileSystem.WriteMode.OVERWRITE);
                }
                return fsDataOutputStream;
            }
            return null;
        }

        public void close4Write() throws IOException {
            fsDataOutputStream.close();
            fsDataOutputStream = null;
            writing = false;
            release();
            scheduleDelete(this);
        }

        public void close4Read() {
            release();
        }

        @Override
        protected void referenceCountReachedZero() {
            try {
                cacheMap.remove(originalPath);
                if (fsDataInputStream != null) {
                    fsDataInputStream.close();
                    fsDataInputStream = null;
                }
                cacheFs.delete(cachePath, false);
            } catch (Exception e) {
                // ignore;
            }
        }
    }
}
