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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.SerializerFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for state descriptors. A {@code StateDescriptor} is used for creating partitioned
 * {@link State} in stateful operations.
 *
 * <p>Subclasses must correctly implement {@link #equals(Object)} and {@link #hashCode()}.
 *
 * @param <S> The type of the State objects created from this {@code StateDescriptor}.
 * @param <T> The type of the value of the state object described by this state descriptor.
 */
@PublicEvolving
public abstract class StateDescriptor<S extends State, T> extends StateDescriptorBase<T> {

    /**
     * Create a new {@code StateDescriptor} with the given name and the given type serializer.
     *
     * @param name The name of the {@code StateDescriptor}.
     * @param serializer The type serializer for the values in the state.
     * @param defaultValue The default value that will be set when requesting state without setting
     *     a value before.
     */
    protected StateDescriptor(String name, TypeSerializer<T> serializer, @Nullable T defaultValue) {
        super(name, serializer, defaultValue);
    }

    /**
     * Create a new {@code StateDescriptor} with the given name and the given type information.
     *
     * @param name The name of the {@code StateDescriptor}.
     * @param typeInfo The type information for the values in the state.
     * @param defaultValue The default value that will be set when requesting state without setting
     *     a value before.
     */
    protected StateDescriptor(String name, TypeInformation<T> typeInfo, @Nullable T defaultValue) {
        super(name, typeInfo, defaultValue);
    }

    /**
     * Create a new {@code StateDescriptor} with the given name and the given type information.
     *
     * <p>If this constructor fails (because it is not possible to describe the type via a class),
     * consider using the {@link #StateDescriptor(String, TypeInformation, Object)} constructor.
     *
     * @param name The name of the {@code StateDescriptor}.
     * @param type The class of the type of values in the state.
     * @param defaultValue The default value that will be set when requesting state without setting
     *     a value before.
     */
    protected StateDescriptor(String name, Class<T> type, @Nullable T defaultValue) {
        super(name, type, defaultValue);
    }

    @Override
    public final boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (o != null && o.getClass() == this.getClass()) {
            final StateDescriptor<?, ?> that = (StateDescriptor<?, ?>) o;
            return this.name.equals(that.name);
        } else {
            return false;
        }
    }
}
