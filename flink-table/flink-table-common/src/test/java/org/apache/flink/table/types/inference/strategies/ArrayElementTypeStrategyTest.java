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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.inference.TypeStrategiesTestBase;

import java.util.stream.Stream;

/** Tests for {@link ArrayElementTypeStrategy}. */
class ArrayElementTypeStrategyTest extends TypeStrategiesTestBase {

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forStrategy(
                                "Infer an element of an array type",
                                SpecificTypeStrategies.ARRAY_ELEMENT)
                        .inputTypes(DataTypes.ARRAY(DataTypes.BIGINT().notNull()))
                        .expectDataType(DataTypes.BIGINT().nullable()),
                TestSpec.forStrategy(
                                "Infer an element of a multiset type",
                                SpecificTypeStrategies.ARRAY_ELEMENT)
                        .inputTypes(DataTypes.MULTISET(DataTypes.STRING().notNull()))
                        .expectDataType(DataTypes.STRING().nullable()),
                TestSpec.forStrategy(
                                "Error on non collection type",
                                SpecificTypeStrategies.ARRAY_ELEMENT)
                        .inputTypes(DataTypes.BIGINT())
                        .expectErrorMessage(
                                "Could not infer an output type for the given arguments."));
    }
}
