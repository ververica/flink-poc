/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Internal;

import java.io.IOException;

/**
 *  PositionedReadable ClosingFSDataInputStream.
 */
@Internal
public class PositionedReadableClosingFSDataInputStream extends ClosingFSDataInputStream implements PositionedReadable {

    private final PositionedReadable inputStream;
    protected PositionedReadableClosingFSDataInputStream(
            FSDataInputStream delegate,
            SafetyNetCloseableRegistry registry,
            String debugInfo) throws IOException {
        super(delegate, registry, debugInfo);
        assert delegate instanceof PositionedReadable;
        this.inputStream = (PositionedReadable) delegate;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        return inputStream.read(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        inputStream.readFully(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        inputStream.readFully(position, buffer);
    }
}
