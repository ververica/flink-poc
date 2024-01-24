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

package org.apache.flink.api.common.state.async;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.StateDescriptorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import javax.annotation.Nullable;

/**
 * Base class for state descriptors. A {@code StateDescriptor} is used for creating partitioned
 * {@link AsyncValueState} in stateful operations.
 *
 * <p>Subclasses must correctly implement {@link #equals(Object)} and {@link #hashCode()}.
 *
 * @param <S> The type of the State objects created from this {@code StateDescriptor}.
 * @param <T> The type of the value of the state object described by this state descriptor.
 */
@PublicEvolving
public abstract class AsyncStateDescriptor<S extends AsyncState, T> extends StateDescriptorBase<T> {

    /**
     * Create a new {@code StateDescriptor} with the given name and the given type serializer.
     *
     * @param name The name of the {@code StateDescriptor}.
     * @param serializer The type serializer for the values in the state.
     * @param defaultValue The default value that will be set when requesting state without setting
     *     a value before.
     */
    protected AsyncStateDescriptor(String name, TypeSerializer<T> serializer, @Nullable T defaultValue) {
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
    protected AsyncStateDescriptor(String name, TypeInformation<T> typeInfo, @Nullable T defaultValue) {
        super(name, typeInfo, defaultValue);
    }

    /**
     * Create a new {@code StateDescriptor} with the given name and the given type information.
     *
     * <p>If this constructor fails (because it is not possible to describe the type via a class),
     * consider using the {@link #AsyncStateDescriptor(String, TypeInformation, Object)} constructor.
     *
     * @param name The name of the {@code StateDescriptor}.
     * @param type The class of the type of values in the state.
     * @param defaultValue The default value that will be set when requesting state without setting
     *     a value before.
     */
    protected AsyncStateDescriptor(String name, Class<T> type, @Nullable T defaultValue) {
        super(name, type, defaultValue);
    }

    @Override
    public final boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (o != null && o.getClass() == this.getClass()) {
            final AsyncStateDescriptor<?, ?> that = (AsyncStateDescriptor<?, ?>) o;
            return this.name.equals(that.name);
        } else {
            return false;
        }
    }
}
