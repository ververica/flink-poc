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

import org.apache.flink.core.fs.FSDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * ByteBufferReadableFSDataInputStream.
 */
public class ByteBufferReadableFSDataInputStream extends FSDataInputStream {

    private final FSDataInputStream fsdis;

    private final Object lock;

    public ByteBufferReadableFSDataInputStream(FSDataInputStream fsdis) {
        this.fsdis = fsdis;
        this.lock = new Object();
    }

    @Override
    public void seek(long desired) throws IOException {
        fsdis.seek(desired);
    }

    @Override
    public long getPos() throws IOException {
        return fsdis.getPos();
    }

    @Override
    public int read() throws IOException {
        return fsdis.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return fsdis.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return fsdis.read(b, off, len);
    }

    public int read(ByteBuffer bb) throws IOException {
        byte[] tmp = new byte[bb.remaining()];
        int read = fsdis.read(tmp, 0, tmp.length);
        bb.put(tmp, 0, read);
        return read;
    }

    public int read(long position, ByteBuffer bb) throws IOException {
        synchronized (lock) {
            fsdis.seek(position);
            return read(bb);
        }
    }

    @Override
    public long skip(long n) throws IOException {
        return fsdis.skip(n);
    }

    @Override
    public int available() throws IOException {
        return fsdis.available();
    }

    @Override
    public void close() throws IOException {
        fsdis.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        fsdis.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        fsdis.reset();
    }

    @Override
    public boolean markSupported() {
        return fsdis.markSupported();
    }

}
