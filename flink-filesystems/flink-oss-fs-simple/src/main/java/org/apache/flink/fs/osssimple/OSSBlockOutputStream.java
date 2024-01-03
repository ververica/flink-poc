/**
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

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static org.apache.hadoop.fs.aliyun.oss.Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM_KEY;

/**
 * Asynchronous multi-part based uploading mechanism to support huge file
 * which is larger than 5GB. Data will be buffered on local disk, then uploaded
 * to OSS in {@link #close()} method.
 */
public class OSSBlockOutputStream extends OutputStream {
    private static final Logger LOG =
            LoggerFactory.getLogger(OSSBlockOutputStream.class);
    private Configuration conf;
    private boolean closed;
    private String key;
    private File blockFile;
    private Map<Integer, File> blockFiles = new HashMap<>();
    private long blockSize;
    private int blockId = 0;
    private long blockWritten = 0L;
    private String uploadId = null;
    private OutputStream blockStream;
    private final byte[] singleByte = new byte[1];

    private final OSSClient ossClient;

    private final String serverSideEncryptionAlgorithm;

    private final String bucket;

    private final List<ListenableFuture<PartETag>> partETagsFutures;
    private final ListeningExecutorService executorService;

    public OSSBlockOutputStream(Configuration conf,
                                      OSSClient ossClient,
                                      String key,
                                      String bucket,
                                      Long blockSize,
                                ExecutorService executorService
                                ) throws IOException {
        this.conf = conf;
        this.ossClient = ossClient;
        this.key = key;
        this.blockSize = blockSize;
        this.blockFile = newBlockFile();
        this.blockStream =
                new BufferedOutputStream(new FileOutputStream(blockFile));
        this.serverSideEncryptionAlgorithm =
                conf.get(SERVER_SIDE_ENCRYPTION_ALGORITHM_KEY, "");
        this.bucket = bucket;
        this.partETagsFutures = new ArrayList<>(2);
        this.executorService = MoreExecutors.listeningDecorator(executorService);
    }

    private File newBlockFile() throws IOException {
        return AliyunOSSUtils.createTmpFileForWrite(
                String.format("oss-block-%04d-", blockId), blockSize, conf);
    }

    @Override
    public synchronized void flush() throws IOException {
        blockStream.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }

        blockStream.flush();
        blockStream.close();
        if (!blockFiles.values().contains(blockFile)) {
            blockId++;
            blockFiles.put(blockId, blockFile);
        }

        try {
            if (blockFiles.size() == 1) {
                // just upload it directly
                uploadObject(key, blockFile);
            } else {
                if (blockWritten > 0) {
                    ListenableFuture<PartETag> partETagFuture =
                            executorService.submit(() -> {
                                PartETag partETag = uploadPart(blockFile, key, uploadId,
                                        blockId);
                                return partETag;
                            });
                    partETagsFutures.add(partETagFuture);
                }
                // wait for the partial uploads to finish
                final List<PartETag> partETags = waitForAllPartUploads();
                if (null == partETags) {
                    LOG.error("Failed to multipart upload to oss, abort it.");
                    throw new IOException("Failed to multipart upload to oss, abort it.");
                }
                completeMultipartUpload(key, uploadId,
                        new ArrayList<>(partETags));
            }
        } finally {
            removeTemporaryFiles();
            closed = true;
        }
    }

    @Override
    public synchronized void write(int b) throws IOException {
        singleByte[0] = (byte)b;
        write(singleByte, 0, 1);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len)
            throws IOException {
        if (closed) {
            LOG.error("Stream closed.");
            throw new IOException("Stream closed.");
        }
        blockStream.write(b, off, len);
        blockWritten += len;
        if (blockWritten >= blockSize) {
            uploadCurrentPart();
            blockWritten = 0L;
        }
    }

    private void removeTemporaryFiles() {
        for (File file : blockFiles.values()) {
            if (file != null && file.exists() && !file.delete()) {
                LOG.warn("Failed to delete temporary file {}", file);
            }
        }
    }

    private void removePartFiles() throws IOException {
        for (ListenableFuture<PartETag> partETagFuture : partETagsFutures) {
            if (!partETagFuture.isDone()) {
                continue;
            }

            try {
                File blockFile = blockFiles.get(partETagFuture.get().getPartNumber());
                if (blockFile != null && blockFile.exists() && !blockFile.delete()) {
                    LOG.warn("Failed to delete temporary file {}", blockFile);
                }
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Someting happened.", e);
                throw new IOException(e);
            }
        }
    }

    private void uploadCurrentPart() throws IOException {
        blockStream.flush();
        blockStream.close();
        if (blockId == 0) {
            uploadId = getUploadId(key);
        }

        blockId++;
        blockFiles.put(blockId, blockFile);

        File currentFile = blockFile;
        int currentBlockId = blockId;
        ListenableFuture<PartETag> partETagFuture =
                executorService.submit(() -> {
                    PartETag partETag = uploadPart(currentFile, key, uploadId,
                            currentBlockId);
                    return partETag;
                });
        partETagsFutures.add(partETagFuture);
        removePartFiles();
        blockFile = newBlockFile();
        blockStream = new BufferedOutputStream(new FileOutputStream(blockFile));
    }

    /**
     * Block awaiting all outstanding uploads to complete.
     * @return list of results
     * @throws IOException IO Problems
     */
    private List<PartETag> waitForAllPartUploads() throws IOException {
        LOG.debug("Waiting for {} uploads to complete", partETagsFutures.size());
        try {
            return Futures.allAsList(partETagsFutures).get();
        } catch (InterruptedException ie) {
            LOG.warn("Interrupted partUpload", ie);
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException ee) {
            //there is no way of recovering so abort
            //cancel all partUploads
            LOG.debug("While waiting for upload completion", ee);
            LOG.debug("Cancelling futures");
            for (ListenableFuture<PartETag> future : partETagsFutures) {
                future.cancel(true);
            }
            //abort multipartupload
            abortMultipartUpload(key, uploadId);
            LOG.error("Multi-part upload with id '" + uploadId
                    + "' to " + key, ee);
            throw new IOException("Multi-part upload with id '" + uploadId
                    + "' to " + key, ee);
        }
    }

    private void uploadObject(String key, File file) throws IOException {
        File object = file.getAbsoluteFile();
        FileInputStream fis = new FileInputStream(object);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(object.length());
        if (StringUtils.isNotEmpty(serverSideEncryptionAlgorithm)) {
            meta.setServerSideEncryption(serverSideEncryptionAlgorithm);
        }
        try {
            PutObjectResult result = ossClient.putObject(bucket, key, fis, meta);
            LOG.debug(result.getETag());
        } finally {
            fis.close();
        }
    }


    private PartETag uploadPart(File file, String key, String uploadId, int idx)
            throws IOException {
        InputStream instream = null;
        Exception caught = null;
        int tries = 3;
        while (tries > 0) {
            try {
                instream = new FileInputStream(file);
                UploadPartRequest uploadRequest = new UploadPartRequest();
                uploadRequest.setBucketName(bucket);
                uploadRequest.setKey(key);
                uploadRequest.setUploadId(uploadId);
                uploadRequest.setInputStream(instream);
                uploadRequest.setPartSize(file.length());
                uploadRequest.setPartNumber(idx);
                UploadPartResult uploadResult = ossClient.uploadPart(uploadRequest);
                return uploadResult.getPartETag();
            } catch (Exception e) {
                LOG.debug("Failed to upload "+ file.getPath() +", " +
                        "try again.", e);
                caught = e;
            } finally {
                if (instream != null) {
                    instream.close();
                    instream = null;
                }
            }
            tries--;
        }

        assert (caught != null);
        LOG.error("Failed to upload " + file.getPath() +
                " for 3 times.", caught);
        throw new IOException("Failed to upload " + file.getPath() +
                " for 3 times.", caught);
    }

    private void abortMultipartUpload(String key, String uploadId) {
        AbortMultipartUploadRequest request = new AbortMultipartUploadRequest(
                bucket, key, uploadId);
        ossClient.abortMultipartUpload(request);
    }

    private CompleteMultipartUploadResult completeMultipartUpload(String key,
                                                                 String uploadId, List<PartETag> partETags) {
        Collections.sort(partETags, new PartNumberAscendComparator());
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(bucket, key, uploadId,
                        partETags);
        return ossClient.completeMultipartUpload(completeMultipartUploadRequest);
    }

    private static class PartNumberAscendComparator
            implements Comparator<PartETag>, Serializable {
        @Override
        public int compare(PartETag o1, PartETag o2) {
            if (o1.getPartNumber() > o2.getPartNumber()) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    public String getUploadId(String key) {
        InitiateMultipartUploadRequest initiateMultipartUploadRequest =
                new InitiateMultipartUploadRequest(bucket, key);
        InitiateMultipartUploadResult initiateMultipartUploadResult =
                ossClient.initiateMultipartUpload(initiateMultipartUploadRequest);
        return initiateMultipartUploadResult.getUploadId();
    }

}
