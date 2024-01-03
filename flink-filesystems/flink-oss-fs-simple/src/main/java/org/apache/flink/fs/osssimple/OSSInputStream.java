/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fs.osssimple;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GetObjectRequest;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;

import static org.apache.hadoop.fs.aliyun.oss.Constants.*;

/**
 * The input stream for OSS blob system.
 * The class uses multi-part downloading to read data from the object content
 * stream.
 */
public class OSSInputStream extends FSInputStream {
    public static final Logger LOG = LoggerFactory.getLogger(OSSInputStream.class);

    private static final int INVALID_POS = Integer.MIN_VALUE;
    private final long downloadPartSize;
    private OSSClient client;

    private final String bucket;
    private final String key;
    private boolean closed;
    private long contentLength;
    private long position;
    private int bufferSize;

    private int bufferPos;
    private byte[] buffer;

    public OSSInputStream(
            Configuration conf, String bucket,
            OSSClient client, String key, Long contentLength) throws IOException {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.contentLength = contentLength;
        this.downloadPartSize = (int) conf.getLong(
                MULTIPART_DOWNLOAD_SIZE_KEY,
                MULTIPART_DOWNLOAD_SIZE_DEFAULT);
        buffer = new byte[(int) downloadPartSize];
        this.bufferPos = INVALID_POS;

        closed = false;
    }

    /**
     * Reopen the wrapped stream at give position, by seeking for
     * data of a part length from object content stream.
     *
     * @param pos position from start of a file
     * @throws IOException if failed to reopen
     */
    private synchronized void reopen(long pos) throws IOException {
        long partSize;

        if (pos < 0) {
            LOG.error("Cannot seek at negative position:" + pos);
            throw new EOFException("Cannot seek at negative position:" + pos);
        } else if (pos > contentLength) {
            LOG.error("Cannot seek after EOF, contentLength:" +
                    contentLength + " position:" + pos);
            throw new EOFException("Cannot seek after EOF, contentLength:" +
                    contentLength + " position:" + pos);
        } else if (pos + downloadPartSize > contentLength) {
            partSize = contentLength - pos;
        } else {
            partSize = downloadPartSize;
        }

        readFully(buffer, key, pos, pos + partSize - 1);
        position = pos;
        bufferSize = (int) partSize;
        bufferPos = 0;
    }

    @Override
    public synchronized int read() throws IOException {
        checkNotClosed();

        if (!validBuffer() && position < contentLength) {
            reopen(position);
        }

        int byteRead = -1;
        if (validBuffer()) {
            byteRead = buffer[bufferPos] & 0xFF;
        }
        if (byteRead >= 0) {
            position++;
            bufferPos++;
        }

        return byteRead;
    }


    /**
     * Verify that the input stream is open. Non blocking; this gives
     * the last state of the volatile {@link #closed} field.
     *
     * @throws IOException if the connection is closed.
     */
    private void checkNotClosed() throws IOException {
        if (closed) {
            LOG.error("Stream closed");
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len)
            throws IOException {
        checkNotClosed();

        if (buf == null) {
            LOG.error("Stream NullPointerException");
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > buf.length - off) {
            LOG.error("Stream IndexOutOfBoundsException");
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        if (len > 2048 && off == 0) {
            int length = (int) Math.min(contentLength - position, len);
            if (length <= 0) {
                return -1;
            }
            readFully(buf, key, position, position + length - 1);
            seek(position + length);
            return length;
        }

        int bytesRead = 0;
        // Not EOF, and read not done
        while (position < contentLength && bytesRead < len) {
            if (!validBuffer()) {
                reopen(position);
            }

            while (validBuffer()) {
                buf[off + bytesRead] = this.buffer[bufferPos];
                bytesRead++;
                bufferPos++;
                position++;
                if (off + bytesRead >= len) {
                    break;
                }
            }
        }

        // Read nothing, but attempt to read something
        if (bytesRead == 0 && len > 0) {
            return -1;
        } else {
            return bytesRead;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        this.buffer = null;
    }

    @Override
    public synchronized int available() throws IOException {
        checkNotClosed();

        long remaining = contentLength - position;
        if (remaining > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) remaining;
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
        checkNotClosed();
        if (position == pos) {
            return;
        } else if (bufferPos != INVALID_POS) {
            bufferPos += (pos - position);
        }
        position = pos;
    }

    @Override
    public synchronized long getPos() throws IOException {
        checkNotClosed();
        return position;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        checkNotClosed();
        return false;
    }

    private boolean validBuffer() {
        return bufferPos >= 0 && bufferPos < bufferSize;
    }

    public void readFully(byte[] buffer, String key, long byteStart, long byteEnd) {
        try (InputStream in = retrieve(
                key, byteStart, byteEnd)) {
            IOUtils.readFully(in, buffer,
                    0, (int) (byteEnd - byteStart) + 1);

        } catch (Exception e) {
            LOG.warn("Exception thrown when retrieve key: "
                    + this.key + ", exception: " + e);
        }
    }

    public InputStream retrieve(String key, long byteStart, long byteEnd) {
        try {
            GetObjectRequest request = new GetObjectRequest(bucket, key);
            request.setRange(byteStart, byteEnd);
            InputStream in = client.getObject(request).getObjectContent();
            return in;
        } catch (OSSException | ClientException e) {
            LOG.error("Exception thrown when store retrieves key: "
                    + key + ", exception: " + e);
            return null;
        }
    }
}
