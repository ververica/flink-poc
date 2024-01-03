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

package org.apache.flink.fs.osssimple;

import org.apache.flink.core.fs.FSDataInputStream;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Concrete implementation of the {@link FSDataInputStream} for Hadoop's input streams. This
 * supports all file systems supported by Hadoop, such as HDFS and S3 (S3a/S3n).
 */
public final class HadoopDataInputStreamWithForceSeek extends FSDataInputStream {

    /** The internal stream. */
    private final org.apache.hadoop.fs.FSDataInputStream fsDataInputStream;

    /**
     * Creates a new data input stream from the given Hadoop input stream.
     *
     * @param fsDataInputStream The Hadoop input stream
     */
    public HadoopDataInputStreamWithForceSeek(org.apache.hadoop.fs.FSDataInputStream fsDataInputStream) {
        this.fsDataInputStream = checkNotNull(fsDataInputStream);
    }

    @Override
    public void seek(long seekPos) throws IOException {
        fsDataInputStream.seek(seekPos);
    }

    @Override
    public long getPos() throws IOException {
        return fsDataInputStream.getPos();
    }

    @Override
    public int read() throws IOException {
        return fsDataInputStream.read();
    }

    @Override
    public void close() throws IOException {
        fsDataInputStream.close();
    }

    @Override
    public int read(@Nonnull byte[] buffer, int offset, int length) throws IOException {
        return fsDataInputStream.read(buffer, offset, length);
    }

    @Override
    public int available() throws IOException {
        return fsDataInputStream.available();
    }

    @Override
    public long skip(long n) throws IOException {
        return fsDataInputStream.skip(n);
    }
}
