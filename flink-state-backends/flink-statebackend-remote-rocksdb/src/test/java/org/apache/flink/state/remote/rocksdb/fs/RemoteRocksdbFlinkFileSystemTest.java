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

import org.apache.flink.state.remote.rocksdb.fs.cache.FileBasedCache;

import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * RemoteRocksdbFlinkFileSystemTest.
 */
public class RemoteRocksdbFlinkFileSystemTest {

    @Test
    public void testByteBufferInputStreamAndOutputStream() throws Exception {
        LocalFileSystem fileSystem = new LocalFileSystem();
        Path testFilePathForCache = new Path("/tmp", "cache");
        RemoteRocksdbFlinkFileSystem remoteRocksdbFlinkFileSystem = new RemoteRocksdbFlinkFileSystem(fileSystem, new FileBasedCache(fileSystem, testFilePathForCache, 3000));
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


        // file and cache are same
        byte[] writeBuffer1 = new byte[4000];
        byte[] writeBuffer2 = new byte[4000];
        fileSystem.open(new Path("/tmp/cache/test-1")).read(writeBuffer1);
        fileSystem.open(new Path("/tmp/test-1")).read(writeBuffer2);
        for (int i = 0; i < 4000; i++) {
            Assert.assertEquals(String.format("%d byte should be the same", i), writeBuffer2[i], writeBuffer1[i]);
        }

        Thread.sleep(2800);

        ByteBufferReadableFSDataInputStream inputStream = remoteRocksdbFlinkFileSystem.open(
                testFilePath);

        for (int k = 0; k < 10000; k++) {
            inputStream.seek(0);
            ByteBuffer readBuffer = ByteBuffer.allocate(20);
            for (int i = 0; i < 200; i++) {
                readBuffer.clear();
                readBuffer.position(1);
                readBuffer.limit(17);
                int read = inputStream.readFully(readBuffer);
                Assert.assertEquals(16, read);
                Assert.assertEquals(i, readBuffer.getLong(1));
                Assert.assertEquals(i * 2, readBuffer.getLong(9));
            }

            for (int i = 0; i < 200; i += 2) {
                readBuffer.clear();
                readBuffer.position(1);
                readBuffer.limit(17);
                int read = inputStream.readFully(i * 16L, readBuffer);
                Assert.assertEquals(16, read);
                Assert.assertEquals(i, readBuffer.getLong(1));
                Assert.assertEquals(i * 2, readBuffer.getLong(9));
            }
        }
        Assert.assertFalse(remoteRocksdbFlinkFileSystem.exists(new Path("/tmp/cache/test-1")));
        inputStream.close();
        remoteRocksdbFlinkFileSystem.delete(testFilePath, true);
        Assert.assertFalse(remoteRocksdbFlinkFileSystem.exists(testFilePath));
    }

    @Test
    public void testConcurrentPositionRead() throws Exception {
        LocalFileSystem fileSystem = new LocalFileSystem();
        RemoteRocksdbFlinkFileSystem remoteRocksdbFlinkFileSystem = new RemoteRocksdbFlinkFileSystem(fileSystem, null);
        Path testFilePath = new Path("/tmp", "test-2");
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

        ByteBufferReadableFSDataInputStream inputStream = remoteRocksdbFlinkFileSystem.open(
                testFilePath);
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        for (int index = 0; index < 40; index++) {
            futureList.add(CompletableFuture.runAsync(() -> {
                try {
                    ByteBuffer readBuffer = ByteBuffer.allocate(20);
                    for (int i = 0; i < 200; i += 2) {
                        readBuffer.clear();
                        readBuffer.position(1);
                        readBuffer.limit(17);
                        int read = inputStream.readFully(i * 16L, readBuffer);
                        Assert.assertEquals(16, read);
                        Assert.assertEquals(i, readBuffer.getLong(1));
                        Assert.assertEquals(i * 2, readBuffer.getLong(9));
                    }
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            }));
        }
        FutureUtils.waitForAll(futureList).get();
        inputStream.close();
        remoteRocksdbFlinkFileSystem.delete(testFilePath, true);
        Assert.assertFalse(remoteRocksdbFlinkFileSystem.exists(testFilePath));
    }
}
