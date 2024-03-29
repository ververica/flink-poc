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

package org.apache.flink.table.planner.typeutils;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowTypeUtils}. */
class RowTypeUtilsTest {

    private final RowType srcType =
            RowType.of(
                    new LogicalType[] {new IntType(), new VarCharType(), new BigIntType()},
                    new String[] {"f0", "f1", "f2"});

    @Test
    void testGetUniqueName() {
        assertThat(
                        RowTypeUtils.getUniqueName(
                                Arrays.asList("Dave", "Evan"), Arrays.asList("Alice", "Bob")))
                .isEqualTo(Arrays.asList("Dave", "Evan"));
        assertThat(
                        RowTypeUtils.getUniqueName(
                                Arrays.asList("Bob", "Bob", "Dave", "Alice"),
                                Arrays.asList("Alice", "Bob")))
                .isEqualTo(Arrays.asList("Bob_0", "Bob_1", "Dave", "Alice_0"));
    }

    @Test
    void testProjectRowType() {
        assertThat(RowTypeUtils.projectRowType(srcType, new int[] {0}))
                .isEqualTo(RowType.of(new LogicalType[] {new IntType()}, new String[] {"f0"}));

        assertThat(RowTypeUtils.projectRowType(srcType, new int[] {0, 2}))
                .isEqualTo(
                        RowType.of(
                                new LogicalType[] {new IntType(), new BigIntType()},
                                new String[] {"f0", "f2"}));

        assertThat(RowTypeUtils.projectRowType(srcType, new int[] {0, 1, 2})).isEqualTo(srcType);
    }

    @Test
    void testInvalidProjectRowType() {

        assertThatThrownBy(() -> RowTypeUtils.projectRowType(srcType, new int[] {0, 1, 2, 3}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid projection index: 3");

        assertThatThrownBy(() -> RowTypeUtils.projectRowType(srcType, new int[] {0, 1, 3}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid projection index: 3");

        assertThatThrownBy(() -> RowTypeUtils.projectRowType(srcType, new int[] {0, 0, 0, 0}))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Field names must be unique. Found duplicates");
    }
}
