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

package org.apache.flink.state.remote.rocksdb;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Configuration options for the Remote RocksDB backend. */
public class RemoteRocksDBOptions {

    public enum RemoteRocksDBMode {
        REMOTE, LOCAL
    }

    public static final ConfigOption<RemoteRocksDBMode> REMOTE_ROCKSDB_MODE =
            ConfigOptions.key("state.backend.remote-rocksdb.mode")
                    .enumType(RemoteRocksDBMode.class)
                    .defaultValue(RemoteRocksDBMode.LOCAL)
                    .withDescription("");

    public static final ConfigOption<String> REMOTE_ROCKSDB_WORKING_DIR =
            ConfigOptions.key("state.backend.remote-rocksdb.working-dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<Boolean> REMOTE_ROCKSDB_ENABLE_CACHE_LAYER =
            ConfigOptions.key("state.backend.remote-rocksdb.cache-layer.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("");

    public static final ConfigOption<Integer> REMOTE_ROCKSDB_IO_PARALLELISM =
            ConfigOptions.key("state.backend.remote-rocksdb.io.parallelism")
                    .intType()
                    .defaultValue(16)
                    .withDescription("");

    public static final ConfigOption<Long> REMOTE_ROCKSDB_FS_CACHE_LIVE_MILLS =
            ConfigOptions.key("state.backend.remote-rocksdb.fs-cache.ttl")
                    .longType()
                    .defaultValue(60000L)
                    .withDescription("");

    public static final ConfigOption<Long> REMOTE_ROCKSDB_FS_CACHE_TIMEOUT_MILLS =
            ConfigOptions.key("state.backend.remote-rocksdb.fs-cache.timeout")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("");

    public static final ConfigOption<Long> REMOTE_ROCKSDB_READ_AHEAD_FOR_COMPACTION =
            ConfigOptions.key("state.backend.remote-rocksdb.compaction.read-ahead.size")
                    .longType()
                    .defaultValue(8 * 1024 * 1024L)
                    .withDescription("");

    public static final ConfigOption<Long> REMOTE_ROCKSDB_BLOCK_CACHE_SIZE =
            ConfigOptions.key("state.backend.remote-rocksdb.block-cache.size")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("");

}
