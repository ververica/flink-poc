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

import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

public class CachedDataOutputStream extends FSDataOutputStream {

    private final FileBasedCache.CacheEntry cacheEntry;
    private FSDataOutputStream fsdos;

    public CachedDataOutputStream(FileBasedCache.CacheEntry cacheEntry) throws IOException {
        this.cacheEntry = cacheEntry;
        this.fsdos = cacheEntry.open4Write();
    }

    @Override
    public long getPos() throws IOException {
        return fsdos.getPos();
    }

    @Override
    public void write(int b) throws IOException {
        fsdos.write(b);
    }

    public void write(byte[] b) throws IOException {
        fsdos.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        fsdos.write(b, off, len);
    }

    public void write(ByteBuffer bb) throws IOException {
        if (bb.hasArray()) {
            write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
        } else {
            byte[] tmp = new byte[bb.remaining()];
            bb.get(tmp);
            write(tmp, 0, tmp.length);
        }
    }

    @Override
    public void flush() throws IOException {
        fsdos.flush();
    }

    @Override
    public void sync() throws IOException {
        fsdos.sync();
    }

    @Override
    public void close() throws IOException {
        cacheEntry.close4Write();
    }

}
