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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Optional;

/**
 * Type strategy for {@link BuiltInFunctionDefinitions#ROWTIME} which mirrors the type of the passed
 * rowtime column, but returns {@link LogicalTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE} for a {@code
 * BATCH} mode.
 */
@Internal
public class RowtimeTypeStrategy implements TypeStrategy {

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        final DataType dataType = callContext.getArgumentDataTypes().get(0);
        final LogicalType inputType = dataType.getLogicalType();
        if (inputType.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                || inputType.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            return Optional.of(dataType);
        } else if (inputType.is(LogicalTypeRoot.BIGINT)) {
            final DataType timestampType = DataTypes.TIMESTAMP(3);
            if (dataType.getLogicalType().isNullable()) {
                return Optional.of(timestampType.nullable());
            } else {
                return Optional.of(timestampType.notNull());
            }
        }

        return Optional.empty();
    }
}
