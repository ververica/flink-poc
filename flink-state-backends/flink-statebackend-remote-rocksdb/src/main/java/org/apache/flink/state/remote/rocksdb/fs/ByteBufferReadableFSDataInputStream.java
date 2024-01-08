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

package org.apache.flink.state.remote.rocksdb.fs;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.PositionedReadable;
import org.apache.flink.state.remote.rocksdb.fs.cache.BlockBasedCache;
import org.apache.flink.state.remote.rocksdb.fs.cache.CachedDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * ByteBufferReadableFSDataInputStream.
 */
public class ByteBufferReadableFSDataInputStream extends FSDataInputStream {

    private final Path path;
    private final long totalFileSize;
    private final FSDataInputStream fsdis;

    private volatile CachedDataInputStream cachedDataInputStream;

    private final BlockBasedCache blockBasedCache;

    private final Object lock;

    private volatile long toSeek = -1L;

    private final ConcurrentLinkedQueue<FSDataInputStream> concurrentReadInputStreamPool;

    private final Callable<FSDataInputStream> concurrentInputStreamBuilder;

    private final Metrics metricsReporter;

    public ByteBufferReadableFSDataInputStream(
            Path path,
            FSDataInputStream fsdis,
            BlockBasedCache blockBasedCache,
            CachedDataInputStream cachedDataInputStream,
            Metrics metricsReporter,
            Callable<FSDataInputStream> concurrentInputStreamBuilder,
            long totalFileSize) {
        this.path = path;
        this.fsdis = fsdis;
        this.blockBasedCache = blockBasedCache;
        this.cachedDataInputStream = cachedDataInputStream;
        this.metricsReporter = metricsReporter;
        this.lock = new Object();
        this.concurrentReadInputStreamPool = new ConcurrentLinkedQueue<>();
        this.concurrentInputStreamBuilder = concurrentInputStreamBuilder;
        this.totalFileSize = totalFileSize;
    }

    private void seedIfNeeded() throws IOException {
        if (toSeek >= 0) {
            fsdis.seek(toSeek);
            toSeek = -1L;
        }
    }

    @Override
    public void seek(long desired) throws IOException {
        if (cachedDataInputStream != null) {
            if (cachedDataInputStream.isAvailable()) {
                cachedDataInputStream.seek(desired);
            } else {
                cachedDataInputStream.close();
                cachedDataInputStream = null;
            }
        }
        toSeek = desired;
    }

    @Override
    public long getPos() throws IOException {
        if (toSeek >= 0) {
            return toSeek;
        }
        return fsdis.getPos();
    }

    @Override
    public int read() throws IOException {
        seedIfNeeded();
        return fsdis.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        seedIfNeeded();
        return fsdis.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        seedIfNeeded();
        return fsdis.read(b, off, len);
    }

    /**
     * Return the total number of bytes read into the buffer.
     * REQUIRES: External synchronization
     */
    public int readFully(ByteBuffer bb) throws IOException {
        Optional<Integer> result = tryReadFromCache(bb);
        if (result.isPresent()) {
            metricsReporter.hit();
            return result.get();
        }
        metricsReporter.miss();
        seedIfNeeded();
        return readFullyFromFSDataInputStream(fsdis, bb);
    }

    private Optional<Integer> tryReadFromCache(ByteBuffer bb) throws IOException {
        if (cachedDataInputStream != null) {
            if (cachedDataInputStream.isAvailable()) {
                if (toSeek < 0) {
                    toSeek = fsdis.getPos();
                }
                toSeek += bb.remaining();
                if (blockBasedCache != null) {
                    long pos = cachedDataInputStream.getPos();
                    Tuple2<byte[], Integer> ret = cachedDataInputStream.readFullyAndReturnBytes(bb);
                    if (ret.f1 > 0) {
                        Tuple3<Path, Long, Integer> cacheKey = Tuple3.of(path, pos, ret.f1);
                        synchronized (blockBasedCache) {
                            blockBasedCache.put(cacheKey, ret.f0);
                        }
                    }
                    return Optional.of(ret.f1);
                } else {
                    int ret = cachedDataInputStream.readFully(bb);
                    return Optional.of(ret);
                }
            } else {
                cachedDataInputStream.close();
                cachedDataInputStream = null;
            }
        }
        return Optional.empty();
    }

    private int readFullyFromFSDataInputStream(FSDataInputStream fsdis, ByteBuffer bb) throws IOException {
        byte[] tmp = new byte[bb.remaining()];
        int n = 0;
        long pos = fsdis.getPos();
        while (n < tmp.length) {
            int read = fsdis.read(tmp, n, tmp.length - n);
            if (read == -1) {
                break;
            }
            n += read;
        }
        if (n > 0) {
            bb.put(tmp, 0, n);
            if (blockBasedCache != null) {
                Tuple3<Path, Long, Integer> cacheKey = Tuple3.of(path, pos, n);;
                if (n != tmp.length) {
                    byte[] tmp2 = new byte[n];
                    System.arraycopy(tmp, 0, tmp2, 0, n);
                    tmp = tmp2;
                }
                synchronized (blockBasedCache) {
                    blockBasedCache.put(cacheKey, tmp);
                }
            }
        }
        return n;
    }

    /**
     * Return the total number of bytes read into the buffer.
     * Safe for concurrent use by multiple threads.
     */
    public int readFully(long position, ByteBuffer bb) throws IOException {
        int len = Math.min(bb.remaining(), (int) (totalFileSize - position));
        Tuple3<Path, Long, Integer> cacheKey = null;
        if (blockBasedCache != null) {
            cacheKey = Tuple3.of(path, position, len);
            synchronized (blockBasedCache) {
                byte[] cached = blockBasedCache.get(cacheKey);
                if (cached != null) {
                    bb.put(cached);
                    return Math.min(len, cached.length);
                }
            }
        }
        synchronized (lock) {
            if (cachedDataInputStream != null) {
                if (cachedDataInputStream.isAvailable()) {
                    cachedDataInputStream.seek(position);
                    Optional<Integer> result = tryReadFromCache(bb);
                    if (result.isPresent()) {
                        metricsReporter.hit();
                        return result.get();
                    }
                } else {
                    cachedDataInputStream.close();
                    cachedDataInputStream = null;
                }
            }
        }

        metricsReporter.miss();

        FSDataInputStream cacheRemoteStream;
        while ((cacheRemoteStream = concurrentReadInputStreamPool.poll()) == null) {
            try {
                if (concurrentReadInputStreamPool.size() < 65) {
                    cacheRemoteStream = concurrentInputStreamBuilder.call();
                    break;
                }
                Thread.sleep(10);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        if (cacheRemoteStream instanceof PositionedReadable) {
            byte[] tmp = new byte[len];
            ((PositionedReadable) cacheRemoteStream).readFully(position, tmp, 0, tmp.length);
            if (blockBasedCache != null) {
                synchronized (blockBasedCache) {
                    blockBasedCache.put(cacheKey, tmp);
                }
            }
            bb.put(tmp);
            concurrentReadInputStreamPool.offer(cacheRemoteStream);
            return tmp.length;
        }

        cacheRemoteStream.seek(position);
        int result =  readFullyFromFSDataInputStream(cacheRemoteStream, bb);
        concurrentReadInputStreamPool.offer(cacheRemoteStream);
        return result;
    }

    @Override
    public long skip(long n) throws IOException {
        seek(getPos() + n);
        return getPos();
    }

    @Override
    public int available() throws IOException {
        seedIfNeeded();
        return fsdis.available();
    }

    @Override
    public void close() throws IOException {
        fsdis.close();
        for (FSDataInputStream inputStream : concurrentReadInputStreamPool) {
            inputStream.close();
        }
    }

    @Override
    public synchronized void mark(int readlimit) {
        fsdis.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        toSeek = -1L;
        fsdis.reset();
    }

    @Override
    public boolean markSupported() {
        return fsdis.markSupported();
    }

    interface Metrics {
        void hit();

        void miss();
    }

}
