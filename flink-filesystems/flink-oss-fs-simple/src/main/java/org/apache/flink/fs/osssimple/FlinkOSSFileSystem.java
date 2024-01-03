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


import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.OSSObjectSummary;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.comm.Protocol;
import com.aliyun.oss.common.utils.VersionInfoUtils;
import com.aliyun.oss.model.CannedAccessControlList;
import com.aliyun.oss.model.GenericRequest;
import com.aliyun.oss.model.ListObjectsV2Request;
import com.aliyun.oss.model.ObjectMetadata;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.flink.runtime.fs.hdfs.HadoopDataOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils;
import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.apache.hadoop.fs.aliyun.oss.OSSListRequest;
import org.apache.hadoop.fs.aliyun.oss.OSSListResult;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.fs.aliyun.oss.AliyunOSSUtils.objectRepresentsDirectory;
import static org.apache.hadoop.fs.aliyun.oss.Constants.KEEPALIVE_TIME_DEFAULT;
import static org.apache.hadoop.fs.aliyun.oss.Constants.KEEPALIVE_TIME_KEY;
import static org.apache.hadoop.fs.aliyun.oss.Constants.MAX_PAGING_KEYS_DEFAULT;
import static org.apache.hadoop.fs.aliyun.oss.Constants.MAX_PAGING_KEYS_KEY;
import static org.apache.hadoop.fs.aliyun.oss.Constants.MULTIPART_UPLOAD_PART_SIZE_DEFAULT;
import static org.apache.hadoop.fs.aliyun.oss.Constants.MULTIPART_UPLOAD_PART_SIZE_KEY;
import static org.apache.hadoop.fs.aliyun.oss.Constants.UPLOAD_ACTIVE_BLOCKS_DEFAULT;
import static org.apache.hadoop.fs.aliyun.oss.Constants.UPLOAD_ACTIVE_BLOCKS_KEY;
import static org.apache.flink.fs.osssimple.OSSFileSystemUtil.makeQualified;

/**
 * Implementation of the Flink {@link FileSystem} interface for Aliyun OSS.
 * A more simple implementation.
 */
public class FlinkOSSFileSystem extends FileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkOSSFileSystem.class);

    String bucket;

    URI uri;

    String username;

    Path workingDir;

    private OSSClient ossClient;

    private int maxKeys;

    private final Configuration conf;

    private BlockingThreadPoolExecutorService boundedThreadPool;

    private int blockOutputActiveBlocks;

    private final FileStatusCache fileStatusCache = new FileStatusCache((k) -> k.endsWith("sst"));

    FlinkOSSFileSystem(URI name, Configuration conf) throws IOException {
        this.bucket = name.getHost();
        this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());
        this.username = UserGroupInformation.getCurrentUser().getShortUserName();
        this.workingDir = makeQualified(new Path("/user", this.username), this.uri, null);
        this.conf = conf;

        initialize(name, conf);
        LOG.info("Simple OSS FileSystem created.");
    }

    public void initialize(URI uri, Configuration conf) throws IOException {
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setMaxConnections(conf.getInt("fs.oss.connection.maximum", 32));
        boolean secureConnections = conf.getBoolean("fs.oss.connection.secure.enabled", true);
        clientConf.setProtocol(secureConnections ? Protocol.HTTPS : Protocol.HTTP);
        clientConf.setMaxErrorRetry(conf.getInt("fs.oss.attempts.maximum", 10));
        clientConf.setConnectionTimeout(conf.getInt("fs.oss.connection.establish.timeout", 50000));
        clientConf.setSocketTimeout(conf.getInt("fs.oss.connection.timeout", 200000));
        clientConf.setUserAgent(conf.get("fs.oss.user.agent.prefix",
                VersionInfoUtils.getDefaultUserAgent()) + ", Hadoop/" + VersionInfo.getVersion());
        String proxyHost = conf.getTrimmed("fs.oss.proxy.host", "");
        int proxyPort = conf.getInt("fs.oss.proxy.port", -1);
        String proxyUsername;
        String cannedACLName;
        if (StringUtils.isNotEmpty(proxyHost)) {
            clientConf.setProxyHost(proxyHost);
            if (proxyPort >= 0) {
                clientConf.setProxyPort(proxyPort);
            } else if (secureConnections) {
                LOG.warn("Proxy host set without port. Using HTTPS default 443");
                clientConf.setProxyPort(443);
            } else {
                LOG.warn("Proxy host set without port. Using HTTP default 80");
                clientConf.setProxyPort(80);
            }

            proxyUsername = conf.getTrimmed("fs.oss.proxy.username");
            String proxyPassword = conf.getTrimmed("fs.oss.proxy.password");
            if (proxyUsername == null != (proxyPassword == null)) {
                cannedACLName = "Proxy error: fs.oss.proxy.username or fs.oss.proxy.password set without the other.";
                LOG.error(cannedACLName);
                throw new IllegalArgumentException(cannedACLName);
            }

            clientConf.setProxyUsername(proxyUsername);
            clientConf.setProxyPassword(proxyPassword);
            clientConf.setProxyDomain(conf.getTrimmed("fs.oss.proxy.domain"));
            clientConf.setProxyWorkstation(conf.getTrimmed("fs.oss.proxy.workstation"));
        } else if (proxyPort >= 0) {
            proxyUsername = "Proxy error: fs.oss.proxy.port set without fs.oss.proxy.host";
            LOG.error(proxyUsername);
            throw new IllegalArgumentException(proxyUsername);
        }

        proxyUsername = conf.getTrimmed("fs.oss.endpoint", "");
        if (StringUtils.isEmpty(proxyUsername)) {
            LOG.error("Aliyun OSS endpoint should not be null or empty. Please set proper endpoint with 'fs.oss.endpoint'.");
            throw new IllegalArgumentException("Aliyun OSS endpoint should not be null or empty. Please set proper endpoint with 'fs.oss.endpoint'.");
        } else {
            CredentialsProvider provider = AliyunOSSUtils.getCredentialsProvider(uri, conf);
            this.ossClient = new OSSClient(proxyUsername, provider, clientConf);
            cannedACLName = conf.get("fs.oss.acl.default", "");
            if (StringUtils.isNotEmpty(cannedACLName)) {
                CannedAccessControlList cannedACL = CannedAccessControlList.valueOf(cannedACLName);
                this.ossClient.setBucketAcl(this.bucket, cannedACL);
            }

            this.maxKeys = conf.getInt("fs.oss.paging.maximum", 1000);
        }

        long keepAliveTime = conf.getLong(KEEPALIVE_TIME_KEY, KEEPALIVE_TIME_DEFAULT);

        maxKeys = conf.getInt(MAX_PAGING_KEYS_KEY, MAX_PAGING_KEYS_DEFAULT);

        int threadNum = AliyunOSSUtils.intPositiveOption(conf,
                Constants.MULTIPART_DOWNLOAD_THREAD_NUMBER_KEY,
                Constants.MULTIPART_DOWNLOAD_THREAD_NUMBER_DEFAULT);

        int totalTasks = AliyunOSSUtils.intPositiveOption(conf,
                Constants.MAX_TOTAL_TASKS_KEY, Constants.MAX_TOTAL_TASKS_DEFAULT);

        this.boundedThreadPool = BlockingThreadPoolExecutorService.newInstance(
                threadNum, totalTasks, keepAliveTime, TimeUnit.SECONDS,
                "oss-transfer-shared");

        this.blockOutputActiveBlocks = conf.getInt(UPLOAD_ACTIVE_BLOCKS_KEY, UPLOAD_ACTIVE_BLOCKS_DEFAULT);
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public Path getHomeDirectory() {
        return makeQualified(new Path("/user/" + System.getProperty("user.name")), this.uri, null);
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        Path qualifiedPath = makeQualified(f, uri, workingDir);
        String key = pathToKey(qualifiedPath);

        // hack for cache
        FileStatus fs = fileStatusCache.getFileStatus(key);
        if (fs != null) {
            return fs;
        }

        // Root always exists
        if (key.length() == 0) {
            return new OSSFileStatus(0, true, 1, 0, 0, qualifiedPath, username);
        }

        ObjectMetadata meta = getObjectMetadata(key);
        // If key not found and key does not end with "/"
        if (meta == null && !key.endsWith("/")) {
            // In case of 'dir + "/"'
            key += "/";
            meta = getObjectMetadata(key);
        }
        if (meta == null) {
            OSSListRequest listRequest = createListObjectsRequest(key,
                    maxKeys, null, null, false);
            OSSListResult listing = listObjects(listRequest);
            do {
                if (CollectionUtils.isNotEmpty(listing.getObjectSummaries()) ||
                        CollectionUtils.isNotEmpty(listing.getCommonPrefixes())) {
                    return new OSSFileStatus(0, true, 1, 0, 0, qualifiedPath, username);
                } else if (listing.isTruncated()) {
                    listing = continueListObjects(listRequest, listing);
                } else {
                    throw new FileNotFoundException(
                            f + ": No such file or directory!");
                }
            } while (true);
        } else if (objectRepresentsDirectory(key, meta.getContentLength())) {
            return new OSSFileStatus(0, true, 1, 0, meta.getLastModified().getTime(),
                    qualifiedPath, username);
        } else {
            fs = new OSSFileStatus(meta.getContentLength(), false, 1,
                    32 * 1024 * 1024, meta.getLastModified().getTime(),
                    qualifiedPath, username);
            fileStatusCache.addFile(key, fs);
            return fs;
        }
    }

    @Override
    public BlockLocation[] getFileBlockLocations(
            FileStatus file,
            long start,
            long len) throws IOException {
        return new BlockLocation[0];
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        final FileStatus fileStatus = getFileStatus(path);
        if (fileStatus.isDir()) {
            LOG.error("Can't open " + path +
                    " because it is a directory");
            throw new FileNotFoundException("Can't open " + path +
                    " because it is a directory");
        }

        return new HadoopDataInputStreamWithForceSeek(new org.apache.hadoop.fs.FSDataInputStream(new OSSInputStream(conf, bucket,
                ossClient, pathToKey(path), fileStatus.getLen())));
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        return open(f, conf.getInt("fs.oss.input-buffer.size", 16 * 1024));
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        String key = pathToKey(path);
        if (LOG.isDebugEnabled()) {
            LOG.debug("List status for path: " + path);
        }

        final List<FileStatus> result = new ArrayList<FileStatus>();
        final FileStatus fileStatus = getFileStatus(path);

        if (fileStatus.isDir()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("listStatus: doing listObjects for directory " + key);
            }

            OSSListRequest listRequest = createListObjectsRequest(key,
                    maxKeys, null, null, false);
            OSSListResult objects = listObjects(listRequest);
            while (true) {
                for (OSSObjectSummary objectSummary : objects.getObjectSummaries()) {
                    String objKey = objectSummary.getKey();
                    if (objKey.equals(key + "/")) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Ignoring: " + objKey);
                        }
                        continue;
                    } else {
                        Path keyPath =
                                makeQualified(keyToPath(objectSummary.getKey()), uri, workingDir);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Adding: fi: " + keyPath);
                        }
                        result.add(new OSSFileStatus(objectSummary.getSize(), false, 1,
                                32 * 1024 * 1024,
                                objectSummary.getLastModified().getTime(), keyPath, username));
                    }
                }

                for (String prefix : objects.getCommonPrefixes()) {
                    if (prefix.equals(key + "/")) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Ignoring: " + prefix);
                        }
                        continue;
                    } else {
                        Path keyPath = makeQualified(keyToPath(prefix), uri, workingDir);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Adding: rd: " + keyPath);
                        }
                        result.add(getFileStatus(keyPath));
                    }
                }

                if (objects.isTruncated()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("listStatus: list truncated - getting next batch");
                    }
                    objects = continueListObjects(listRequest, objects);
                } else {
                    break;
                }
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Adding: rd (not a dir): " + path);
            }
            result.add(fileStatus);
        }

        return result.toArray(new FileStatus[result.size()]);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        FileStatus status = getFileStatus(path);
        Path f = status.getPath();
        String p = f.toUri().getPath();
        FileStatus[] statuses;
        // indicating root directory "/".
        if (p.equals("/")) {
            statuses = listStatus(status.getPath());
            boolean isEmptyDir = statuses.length <= 0;
            return rejectRootDirectoryDelete(isEmptyDir, recursive);
        }

        String key = pathToKey(f);
        if (status.isDir()) {
            if (!recursive) {
                // Check whether it is an empty directory or not
                statuses = listStatus(status.getPath());
                if (statuses.length > 0) {
                    LOG.error("Cannot remove directory " + f +
                            ": It is not empty!");
                    throw new IOException("Cannot remove directory " + f +
                            ": It is not empty!");
                } else {
                    // Delete empty directory without '-r'
                    key = AliyunOSSUtils.maybeAddTrailingSlash(key);
                    deleteObject(key);
                }
            } else {
                deleteDirs(key);
            }
        } else {
            deleteObject(key);
        }

        createFakeDirectoryIfNecessary(f);
        return true;
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        try {
            FileStatus fileStatus = getFileStatus(path);

            if (fileStatus.isDir()) {
                return true;
            } else {
                LOG.error("Path is a file: " + path);
                throw new FileAlreadyExistsException("Path is a file: " + path);
            }
        } catch (FileNotFoundException e) {
            validatePath(path);
            String key = pathToKey(path);
            return mkdir(key);
        }
    }

    @Override
    public FSDataOutputStream create(Path path, WriteMode overwriteMode) throws IOException {
        String key = pathToKey(path);
        FileStatus status = null;

        try {
            try {
                // get the status or throw a FNFE
                status = getFileStatus(path);

                // if the thread reaches here, there is something at the path
                if (status.isDir()) {
                    LOG.error(path + " is a directory");
                    // path references a directory
                    throw new FileAlreadyExistsException(path + " is a directory");
                }
                if (overwriteMode != WriteMode.OVERWRITE) {
                    LOG.error(path + " already exists");
                    // path references a file and overwrite is disabled
                    throw new FileAlreadyExistsException(path + " already exists");
                }
                LOG.info("Overwriting file {}", path);
            } catch (FileNotFoundException e) {
                // this means the file is not found
            }

            long uploadPartSize = AliyunOSSUtils.getMultipartSizeProperty(conf,
                    MULTIPART_UPLOAD_PART_SIZE_KEY, MULTIPART_UPLOAD_PART_SIZE_DEFAULT);
            return new HadoopDataOutputStream(
                    new org.apache.hadoop.fs.FSDataOutputStream(
                            new OSSBlockOutputStream(
                                    conf,
                                    ossClient,
                                    key,
                                    bucket,
                                    uploadPartSize,
                                    new SemaphoredDelegatingExecutor(boundedThreadPool,
                                            blockOutputActiveBlocks, true)), null));
        } catch (Throwable e) {
            e.printStackTrace();
            LOG.error("Fail when create", e);
        }
        return null;
    }

    @Override
    public boolean rename(Path srcPath, Path dstPath) throws IOException {
        if (srcPath.getParent() == null) {
            // Cannot rename root of file system
            LOG.error("Cannot rename the root of a filesystem");
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cannot rename the root of a filesystem");
            }
            return false;
        }
        Path parent = dstPath.getParent();
        while (parent != null && !srcPath.equals(parent)) {
            parent = parent.getParent();
        }
        if (parent != null) {
            return false;
        }

        FileStatus srcStatus = getFileStatus(srcPath);
        FileStatus dstStatus;
        try {
            dstStatus = getFileStatus(dstPath);
        } catch (FileNotFoundException fnde) {
            dstStatus = null;
        }
        if (dstStatus == null) {
            // If dst doesn't exist, check whether dst dir exists or not
            dstStatus = getFileStatus(dstPath.getParent());
            if (!dstStatus.isDir()) {
                LOG.error(String.format(
                        "Failed to rename %s to %s, %s is a file", srcPath, dstPath,
                        dstPath.getParent()));
                throw new IOException(String.format(
                        "Failed to rename %s to %s, %s is a file", srcPath, dstPath,
                        dstPath.getParent()));
            }
        } else {
            if (srcStatus.getPath().equals(dstStatus.getPath())) {
                return !srcStatus.isDir();
            } else if (dstStatus.isDir()) {
                // If dst is a directory
                dstPath = new Path(dstPath, srcPath.getName());
                FileStatus[] statuses;
                try {
                    statuses = listStatus(dstPath);
                } catch (FileNotFoundException fnde) {
                    statuses = null;
                }
                if (statuses != null && statuses.length > 0) {
                    LOG.error(String.format(
                            "Failed to rename %s to %s, file already exists or not empty!",
                            srcPath, dstPath));
                    // If dst exists and not a directory / not empty
                    throw new FileAlreadyExistsException(String.format(
                            "Failed to rename %s to %s, file already exists or not empty!",
                            srcPath, dstPath));
                }
            } else {
                // If dst is not a directory
                LOG.error(String.format(
                        "Failed to rename %s to %s, file already exists!", srcPath,
                        dstPath));
                throw new FileAlreadyExistsException(String.format(
                        "Failed to rename %s to %s, file already exists!", srcPath,
                        dstPath));
            }
        }

        boolean succeed;
        if (srcStatus.isDir()) {
            succeed = copyDirectory(srcPath, dstPath);
        } else {
            succeed = copyFile(srcPath, srcStatus.getLen(), dstPath);
        }

        return srcPath.equals(dstPath) || (succeed && delete(srcPath, true));
    }

    @Override
    public boolean isDistributedFS() {
        return true;
    }

    @Override
    public FileSystemKind getKind() {
        return FileSystemKind.OBJECT_STORE;
    }


    private String pathToKey(Path path) {
        if (!path.isAbsolute()) {
            path = new Path(workingDir, path);
        }

        return path.toUri().getPath().substring(1);
    }

    private Path keyToPath(String key) {
        return new Path("/" + key);
    }

    private ObjectMetadata getObjectMetadata(String key) {
        try {
            GenericRequest request = new GenericRequest(bucket, key);
            request.setLogEnabled(false);
            ObjectMetadata objectMeta = this.ossClient.getObjectMetadata(request);
            return objectMeta;
        } catch (OSSException var4) {
            LOG.debug("Exception thrown when get object meta: " + key + ", exception: " + var4);
            return null;
        }
    }

    private OSSListRequest createListObjectsRequest(String prefix, int maxListingLength, String marker, String continuationToken, boolean recursive) {
        String delimiter = recursive ? null : "/";
        prefix = AliyunOSSUtils.maybeAddTrailingSlash(prefix);
        ListObjectsV2Request listV2Request = new ListObjectsV2Request(bucket);
        listV2Request.setPrefix(prefix);
        listV2Request.setDelimiter(delimiter);
        listV2Request.setMaxKeys(maxListingLength);
        listV2Request.setContinuationToken(continuationToken);
        return OSSListRequest.v2(listV2Request);
    }

    private OSSListResult listObjects(OSSListRequest listRequest) {
        OSSListResult listResult;
        if (listRequest.isV1()) {
            listResult = OSSListResult.v1(this.ossClient.listObjects(listRequest.getV1()));
        } else {
            listResult = OSSListResult.v2(this.ossClient.listObjectsV2(listRequest.getV2()));
        }

        return listResult;
    }

    private OSSListResult continueListObjects(OSSListRequest listRequest, OSSListResult preListResult) {
        OSSListResult listResult;
        if (listRequest.isV1()) {
            listRequest.getV1().setMarker(preListResult.getV1().getNextMarker());
            listResult = OSSListResult.v1(this.ossClient.listObjects(listRequest.getV1()));
        } else {
            listRequest.getV2().setContinuationToken(preListResult.getV2().getNextContinuationToken());
            listResult = OSSListResult.v2(this.ossClient.listObjectsV2(listRequest.getV2()));
        }
        return listResult;
    }


    private boolean copyFile(Path srcPath, long srcLen, Path dstPath) {
        String srcKey = pathToKey(srcPath);
        String dstKey = pathToKey(dstPath);
        return copyFile(srcKey, srcLen, dstKey);
    }

    private boolean copyFile(String srcKey, long srcLen, String dstKey) {
        try {
            //1, try single copy first
            return singleCopy(srcKey, dstKey);
        } catch (Exception e) {
            //2, if failed(shallow copy not supported), then multi part copy
            LOG.info("Exception thrown when copy file: " + srcKey);
        }
        return false;
    }

    /**
     * Use single copy to copy an OSS object.
     * (The caller should make sure srcPath is a file and dstPath is valid)
     *
     * @param srcKey source key.
     * @param dstKey destination key.
     * @return true if object is successfully copied.
     */
    private boolean singleCopy(String srcKey, String dstKey) {
        CopyObjectResult copyResult =
                ossClient.copyObject(bucket, srcKey, bucket, dstKey);
        LOG.debug(copyResult.getETag());
        return true;
    }

    private boolean copyDirectory(Path srcPath, Path dstPath) throws IOException {
        LOG.error("unsupported copyDirectory");
        throw new RuntimeException("unsupported yet");
    }

    private boolean rejectRootDirectoryDelete(boolean isEmptyDir,
                                              boolean recursive) throws IOException {
        LOG.info("oss delete the {} root directory of {}", bucket, recursive);
        if (isEmptyDir) {
            return true;
        }
        if (recursive) {
            return false;
        } else {
            LOG.error("Cannot delete root path");
            // reject
            throw new PathIOException(bucket, "Cannot delete root path");
        }
    }

    private void createFakeDirectoryIfNecessary(Path f) throws IOException {
        String key = pathToKey(f);
        if (StringUtils.isNotEmpty(key) && !exists(f)) {
            LOG.debug("Creating new fake directory at {}", f);
            mkdir(pathToKey(f.getParent()));
        }
    }

    private boolean mkdir(final String key) throws IOException {
        String dirName = key;
        if (StringUtils.isNotEmpty(key)) {
            if (!key.endsWith("/")) {
                dirName += "/";
            }
            storeEmptyFile(dirName);
        }
        return true;
    }

    private void storeEmptyFile(String key) throws IOException {
        ObjectMetadata dirMeta = new ObjectMetadata();
        byte[] buffer = new byte[0];
        ByteArrayInputStream in = new ByteArrayInputStream(buffer);
        dirMeta.setContentLength(0);
        try {
            ossClient.putObject(bucket, key, in, dirMeta);
        } finally {
            in.close();
        }
    }

    public void deleteObject(String key) {
        fileStatusCache.deleteFile(key);
        ossClient.deleteObject(bucket, key);
    }

    public void deleteDirs(String key) throws IOException {
        OSSListRequest listRequest = createListObjectsRequest(key,
                maxKeys, null, null, true);
        while (true) {
            OSSListResult objects = listObjects(listRequest);
            List<String> keysToDelete = new ArrayList<String>();
            for (OSSObjectSummary objectSummary : objects.getObjectSummaries()) {
                keysToDelete.add(objectSummary.getKey());
            }
            deleteObjects(keysToDelete);
            if (objects.isTruncated()) {
                if (objects.isV1()) {
                    listRequest.getV1().setMarker(objects.getV1().getNextMarker());
                } else {
                    listRequest.getV2().setContinuationToken(
                            objects.getV2().getNextContinuationToken());
                }
            } else {
                break;
            }
        }
    }

    public void deleteObjects(List<String> keysToDelete) throws IOException {
        if (CollectionUtils.isEmpty(keysToDelete)) {
            LOG.warn("Keys to delete is empty.");
            return;
        }

        int retry = 10;
        int tries = 0;
        List<String> deleteFailed = keysToDelete;
        while(CollectionUtils.isNotEmpty(deleteFailed)) {
            DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket);
            deleteRequest.setKeys(deleteFailed);
            // There are two modes to do batch delete:
            // 1. detail mode: DeleteObjectsResult.getDeletedObjects returns objects
            // which were deleted successfully.
            // 2. simple mode: DeleteObjectsResult.getDeletedObjects returns objects
            // which were deleted unsuccessfully.
            // Here, we choose the simple mode to do batch delete.
            deleteRequest.setQuiet(true);
            DeleteObjectsResult result = ossClient.deleteObjects(deleteRequest);
            deleteFailed = result.getDeletedObjects();
            tries++;
            if (tries == retry) {
                break;
            }
        }

        if (tries == retry && CollectionUtils.isNotEmpty(deleteFailed)) {
            // Most of time, it is impossible to try 10 times, expect the
            // Aliyun OSS service problems.

            LOG.error("Failed to delete Aliyun OSS objects for " +
                    tries + " times.");
            throw new IOException("Failed to delete Aliyun OSS objects for " +
                    tries + " times.");
        }
    }

    private void validatePath(Path path) throws IOException {
       Path fPart = path.getParent();
        do {
            try {
                FileStatus fileStatus = getFileStatus(fPart);
                if (fileStatus.isDir()) {
                    // If path exists and a directory, exit
                    break;
                } else {
                    LOG.error(String.format(
                            "Can't make directory for path '%s', it is a file.", fPart));
                    throw new FileAlreadyExistsException(String.format(
                            "Can't make directory for path '%s', it is a file.", fPart));
                }
            } catch (FileNotFoundException fnfe) {
            }
            fPart = fPart.getParent();
        } while (fPart != null);
    }
}
