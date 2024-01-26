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

package org.apache.flink.runtime.state.async;

import org.apache.flink.api.common.state.async.AsyncState;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public interface InternalAsyncState<K, N, V> extends AsyncState {

    /** Returns the {@link TypeSerializer} for the type of key this state is associated to. */
    TypeSerializer<K> getKeySerializer();

    /** Returns the {@link TypeSerializer} for the type of namespace this state is associated to. */
    TypeSerializer<N> getNamespaceSerializer();

    /** Returns the {@link TypeSerializer} for the type of value this state holds. */
    TypeSerializer<V> getValueSerializer();

    /**
     * Sets the current namespace, which will be used when using the state access methods.
     *
     * @param namespace The namespace.
     */
    void setCurrentNamespace(N namespace);

    /**
     * Returns the serialized value for the given key and namespace.
     *
     * <p>If no value is associated with key and namespace, <code>null</code> is returned.
     *
     * <p><b>TO IMPLEMENTERS:</b> This method is called by multiple threads. Anything stateful (e.g.
     * serializers) should be either duplicated or protected from undesired consequences of
     * concurrent invocations.
     *
     * @param serializedKeyAndNamespace Serialized key and namespace
     * @param safeKeySerializer A key serializer which is safe to be used even in multi-threaded
     *     context
     * @param safeNamespaceSerializer A namespace serializer which is safe to be used even in
     *     multi-threaded context
     * @param safeValueSerializer A value serializer which is safe to be used even in multi-threaded
     *     context
     * @return Serialized value or <code>null</code> if no value is associated with the key and
     *     namespace.
     * @throws Exception Exceptions during serialization are forwarded
     */
    byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<V> safeValueSerializer)
            throws Exception;

}
