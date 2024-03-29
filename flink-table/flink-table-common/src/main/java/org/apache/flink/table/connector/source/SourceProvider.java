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

package org.apache.flink.table.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.connector.ParallelismProvider;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * Provider of a {@link Source} instance as a runtime implementation for {@link ScanTableSource}.
 *
 * <p>{@code DataStreamScanProvider} in {@code flink-table-api-java-bridge} is available for
 * advanced connector developers.
 */
@PublicEvolving
public interface SourceProvider extends ScanTableSource.ScanRuntimeProvider, ParallelismProvider {

    /** Helper method for creating a static provider. */
    static SourceProvider of(Source<RowData, ?, ?> source) {
        return of(source, null);
    }

    /** Helper method for creating a Source provider with a provided source parallelism. */
    static SourceProvider of(Source<RowData, ?, ?> source, @Nullable Integer sourceParallelism) {
        return new SourceProvider() {

            @Override
            public Source<RowData, ?, ?> createSource() {
                return source;
            }

            @Override
            public boolean isBounded() {
                return Boundedness.BOUNDED.equals(source.getBoundedness());
            }

            @Override
            public Optional<Integer> getParallelism() {
                return Optional.ofNullable(sourceParallelism);
            }
        };
    }

    /** Creates a {@link Source} instance. */
    Source<RowData, ?, ?> createSource();
}
