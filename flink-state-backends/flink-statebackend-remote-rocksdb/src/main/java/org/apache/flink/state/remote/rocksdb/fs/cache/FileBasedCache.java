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

import org.apache.flink.metrics.MetricGroup;

import org.apache.flink.runtime.state.ReferenceCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FileBasedCache implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(FileBasedCache.class);
    private final FileSystem cacheFs;

    private final Path basePath;

    private final long cacheTtl;

    private final ConcurrentHashMap<Path, CacheEntry> cacheMap;

    private final ScheduledExecutorService timeTickService;

    private volatile boolean closed;

    private final AtomicLong cacheHit = new AtomicLong(0L);

    private final AtomicLong cacheMiss = new AtomicLong(0L);

    private final AtomicLong cacheSize = new AtomicLong(0L);

    public FileBasedCache(FileSystem flinkFs, Path basePath, long cacheTtl, long timeout) {
        this(flinkFs, basePath, cacheTtl, timeout, null);
    }

    public FileBasedCache(FileSystem flinkFs, Path basePath, long cacheTtl, long timeout, MetricGroup metricGroup) {
        this.cacheFs = flinkFs;
        this.basePath = basePath;
        this.cacheTtl = cacheTtl;
        this.cacheMap = new ConcurrentHashMap<>();
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("CacheDeleteTickTimeService-%d").build();
        this.timeTickService = new ScheduledThreadPoolExecutor(1, threadFactory);
        this.closed = false;
        scheduleClose(timeout);
        if (metricGroup != null) {
            metricGroup.gauge("hit", cacheHit::get);
            metricGroup.gauge("miss", cacheMiss::get);
            metricGroup.gauge("size", cacheSize::get);
        }
        LOG.info("Local fs-cache initialized at {} with ttl {} timeout {}.", basePath, cacheTtl, timeout);
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            timeTickService.shutdownNow();
            cacheMap.values().forEach(CacheEntry::invalidate);
            LOG.info("Local fs-cache closed");
        }
    }

    void scheduleDelete(CacheEntry e) {
        timeTickService.schedule(e::invalidate, cacheTtl, TimeUnit.MILLISECONDS);
    }

    void scheduleClose(long timeout) {
        if (timeout > 0L) {
            timeTickService.schedule(this::close, timeout, TimeUnit.MILLISECONDS);
        }
    }

    public CachedDataInputStream open4Read(Path path) throws IOException {
        if (closed) {
            return null;
        }
        CacheEntry entry = cacheMap.get(path);
        if (entry != null) {
            return new CachedDataInputStream(entry);
        } else {
            return null;
        }
    }

    public CachedDataOutputStream open4Write(Path path) throws IOException {
        if (closed) {
            return null;
        }
        CacheEntry entry = new CacheEntry(path, getCachePath(path));
        cacheMap.put(path, entry);
        return new CachedDataOutputStream(entry);
    }

    Path getCachePath(Path fromOriginal) {
        return new Path(basePath, fromOriginal.getName());
    }

    public void reportHit() {
        cacheHit.incrementAndGet();
    }

    public void reportMiss() {
        cacheMiss.incrementAndGet();
    }

    class CacheEntry extends ReferenceCounted {

        private final Path originalPath;

        private final Path cachePath;

        private volatile FSDataInputStream fsDataInputStream;

        private volatile FSDataOutputStream fsDataOutputStream;

        private volatile boolean writing;

        private volatile boolean closed;

        private long entrySize = 0L;

        CacheEntry(Path originalPath, Path cachePath) {
            super(1);
            this.originalPath = originalPath;
            this.cachePath = cachePath;
            this.writing = true;
            this.closed = false;
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
            long thisSize = fsDataOutputStream.getPos();
            fsDataOutputStream.close();
            cacheSize.addAndGet(thisSize);
            entrySize += thisSize;
            fsDataOutputStream = null;
            writing = false;
            release();
            if (!closed) {
                scheduleDelete(this);
            }
        }

        public void close4Read() {
            release();
        }

        public synchronized void invalidate() {
            if (!closed) {
                closed = true;
                release();
            }
        }

        @Override
        protected void referenceCountReachedZero() {
            try {
                cacheMap.remove(originalPath);
                if (fsDataInputStream != null) {
                    fsDataInputStream.close();
                    fsDataInputStream = null;
                }
                cacheSize.addAndGet(-entrySize);
                cacheFs.delete(cachePath, false);
            } catch (Exception e) {
                // ignore;
            }
        }
    }
}
