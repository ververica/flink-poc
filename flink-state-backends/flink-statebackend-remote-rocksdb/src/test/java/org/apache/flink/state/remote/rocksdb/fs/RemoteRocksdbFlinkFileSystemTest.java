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

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * RemoteRocksdbFlinkFileSystemTest.
 */
public class RemoteRocksdbFlinkFileSystemTest {

    @Test
    public void testByteBufferInputStreamAndOutputStream() throws Exception {
        LocalFileSystem fileSystem = new LocalFileSystem();
        RemoteRocksdbFlinkFileSystem remoteRocksdbFlinkFileSystem = new RemoteRocksdbFlinkFileSystem(fileSystem);
        Path testFilePath = new Path("/tmp", "test-1");
        ByteBufferWritableFSDataOutputStream outputStream = remoteRocksdbFlinkFileSystem.create(testFilePath);
        ByteBuffer writeBuffer = ByteBuffer.allocate(20);
        for (int i = 0; i < 200; i++) {
            writeBuffer.clear();
            writeBuffer.position(2);
            writeBuffer.putLong(i);
            writeBuffer.putLong(i * 2);
            writeBuffer.flip();
            writeBuffer.position(2);
            outputStream.write(writeBuffer);
        }
        outputStream.flush();
        outputStream.close();

        ByteBufferReadableFSDataInputStream inputStream = remoteRocksdbFlinkFileSystem.open(testFilePath);
        ByteBuffer readBuffer = ByteBuffer.allocate(20);
        for (int i = 0; i < 200; i++) {
            readBuffer.clear();
            readBuffer.position(1);
            readBuffer.limit(17);
            int read = inputStream.read(readBuffer);
            Assert.assertEquals(16, read);
            Assert.assertEquals(i, readBuffer.getLong(1));
            Assert.assertEquals(i * 2, readBuffer.getLong(9));
        }

        for (int i = 0; i < 200; i +=2) {
            readBuffer.clear();
            readBuffer.position(1);
            readBuffer.limit(17);
            int read = inputStream.read(i * 16L, readBuffer);
            Assert.assertEquals(16, read);
            Assert.assertEquals(i, readBuffer.getLong(1));
            Assert.assertEquals(i * 2, readBuffer.getLong(9));
        }
        inputStream.close();
        remoteRocksdbFlinkFileSystem.delete(testFilePath, true);
        Assert.assertFalse(remoteRocksdbFlinkFileSystem.exists(testFilePath));
    }
}
