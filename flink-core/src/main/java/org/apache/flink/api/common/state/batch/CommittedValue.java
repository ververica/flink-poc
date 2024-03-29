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

package org.apache.flink.api.common.state.batch;

/**
 * CommittedValue.
 */
public class CommittedValue<T> {

    private final T value;

    private final CommittedValueType valueType;

    private CommittedValue(T value, CommittedValueType valueType) {
        this.value = value;
        this.valueType = valueType;
    }

    public CommittedValueType getValueType() {
        return valueType;
    }
    
    public T getValue() {
        return value;
    }

    public static <T> CommittedValue<T> of(T value, CommittedValueType valueType) {
        return new CommittedValue<>(value, valueType);
    }

    public static <T> CommittedValue<T> ofDeletedValue() {
        return new CommittedValue<>(null, CommittedValueType.DELETE);
    }

    public enum CommittedValueType {
        UPDATE, DELETE, UNMODIFIED
    }
}
