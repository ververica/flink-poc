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
import org.apache.flink.api.common.state.async.AsyncStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;

import javax.annotation.Nonnull;

/** This factory produces concrete internal state objects. */
public interface AsyncKeyedStateFactory {

    /**
     * Creates or updates internal state and returns a new {@link AsyncState}.
     *
     * @param namespaceSerializer TypeSerializer for the state namespace.
     * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
     * @param snapshotTransformFactory factory of state snapshot transformer.
     * @param <N> The type of the namespace.
     * @param <SV> The type of the stored state value.
     * @param <SEV> The type of the stored state value or entry for collection types (list or map).
     * @param <S> The type of the public API state.
     * @param <IS> The type of internal state.
     */
    @Nonnull
    default <N, SV, SEV, S extends AsyncState, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull AsyncStateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws Exception {
        throw new UnsupportedOperationException("Error");
    }
}
