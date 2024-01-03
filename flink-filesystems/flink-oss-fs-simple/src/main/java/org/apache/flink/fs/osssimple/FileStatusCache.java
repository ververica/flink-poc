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

import org.apache.flink.core.fs.FileStatus;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class FileStatusCache {

    final ConcurrentHashMap<String, FileStatus> fileStatusMap;

    final Function<String, Boolean> filter;

    public FileStatusCache() {
        this((s) -> true);
    }

    public FileStatusCache(Function<String, Boolean> filter) {
        this.fileStatusMap = new ConcurrentHashMap<>();
        this.filter = filter;
    }

    public FileStatus getFileStatus(String key) {
        if (filter.apply(key)) {
            return fileStatusMap.get(key);
        }
        return null;
    }

    public void deleteFile(String key) {
        if (filter.apply(key)) {
            fileStatusMap.remove(key);
        }
    }


    public void addFile(String key, FileStatus fileStatus) {
        if (filter.apply(key)) {
            fileStatusMap.put(key, fileStatus);
        }
    }
}
