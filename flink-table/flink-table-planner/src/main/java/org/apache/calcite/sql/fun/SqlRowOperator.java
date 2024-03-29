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

package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;

import java.util.AbstractList;

/**
 * Copied to keep null semantics of table api and sql in sync.
 *
 * <p>There are differences following:
 *
 * <p>1. The return value about {@code R IS NULL} and {@code R IS NOT NULL}.
 *
 * <p>At the same time SQL standard says that the next about `ROW`:
 *
 * <ul>
 *   <li>The value of {@code R IS NULL} is:
 *       <ul>
 *         <li>If the value of every field of V is the null value, then True.
 *         <li>Otherwise, False.
 *       </ul>
 *   <li>The value of {@code R IS NOT NULL} is:
 *       <ul>
 *         <li>If the value of no field of V is the null value, then True.
 *         <li>Otherwise, False.
 *       </ul>
 * </ul>
 *
 * <p>Calcite applies that logic since <a
 * href="https://issues.apache.org/jira/browse/CALCITE-3627">CALCITE-3627</a> (1.30.0+).
 *
 * <ul>
 *   <li>Thus, with Calcite 1.30.0+
 *       <ul>
 *         <li>{@code SELECT ROW(CAST(NULL AS INT), CAST(NULL AS INT)) IS NOT NULL; -- returns
 *             FALSE}
 *         <li>{@code SELECT ROW(CAST(NULL AS INT), CAST(NULL AS INT)) IS NULL; -- returns TRUE}.
 *       </ul>
 *   <li>With Flink and Calcite before 1.30.0 (current behavior of this class)
 *       <ul>
 *         <li>{@code SELECT ROW(CAST(NULL AS INT), CAST(NULL AS INT)) IS NOT NULL; -- returns TRUE}
 *         <li>{@code SELECT ROW(CAST(NULL AS INT), CAST(NULL AS INT)) IS NULL; -- returns FALSE}
 *       </ul>
 * </ul>
 *
 * <p>Once Flink applies same logic for both table api and sql, this first changes should be
 * removed.
 *
 * <p>2. It uses {@link StructKind#PEEK_FIELDS_NO_EXPAND} with a nested struct type (Flink [[{@link
 * org.apache.flink.table.types.logical.RowType}]]).
 *
 * <p>See more at {@link org.apache.flink.table.planner.typeutils.LogicalRelDataTypeConverter} and
 * {@link org.apache.flink.table.planner.calcite.FlinkTypeFactory}.
 *
 * <p>Changed lines
 *
 * <ol>
 *   <li>Line 106 ~ 137
 * </ol>
 */
public class SqlRowOperator extends SqlSpecialOperator {
    // ~ Constructors -----------------------------------------------------------

    public SqlRowOperator(String name) {
        super(
                name,
                SqlKind.ROW,
                MDX_PRECEDENCE,
                false,
                null,
                InferTypes.RETURN_TYPE,
                OperandTypes.VARIADIC);
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        // ----- FLINK MODIFICATION BEGIN -----
        // The type of a ROW(e1,e2) expression is a record with the types
        // {e1type,e2type}.  According to the standard, field names are
        // implementation-defined.
        int fieldCount = opBinding.getOperandCount();
        return opBinding
                .getTypeFactory()
                .createStructType(
                        StructKind.PEEK_FIELDS_NO_EXPAND,
                        new AbstractList<RelDataType>() {
                            @Override
                            public RelDataType get(int index) {
                                return opBinding.getOperandType(index);
                            }

                            @Override
                            public int size() {
                                return fieldCount;
                            }
                        },
                        new AbstractList<String>() {
                            @Override
                            public String get(int index) {
                                return SqlUtil.deriveAliasFromOrdinal(index);
                            }

                            @Override
                            public int size() {
                                return fieldCount;
                            }
                        });
        // ----- FLINK MODIFICATION END -----
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlUtil.unparseFunctionSyntax(this, writer, call, false);
    }

    // override SqlOperator
    @Override
    public boolean requiresDecimalExpansion() {
        return false;
    }
}
