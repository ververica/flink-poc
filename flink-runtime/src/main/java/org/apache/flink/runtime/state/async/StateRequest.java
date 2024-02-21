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

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import static org.apache.flink.runtime.state.async.StateRequest.RequestType.GET;
import static org.apache.flink.runtime.state.async.StateRequest.RequestType.PUT;

@Internal
public class StateRequest<S, K, V, F> {

    S state;

    RequestType type;

    K key;

    @Nullable V value;

    InternalStateFuture<F> stateFuture;

    private StateRequest(S state, RequestType type, K key, @Nullable V value, InternalStateFuture<F> stateFuture) {
        this.state = state;
        this.type = type;
        this.key = key;
        this.value = value;
        this.stateFuture = stateFuture;
    }

    public S getState() {
        return state;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public RequestType getQuestType() {
        return type;
    }
    
    public InternalStateFuture<F> getFuture() {
        return stateFuture;
    }

    public static <S, K, V> StateRequest<S, K, Void, V> ofValueGet(
            S state, K key, InternalStateFuture<V> future) {
        return new StateRequest<>(state, GET, key, null, future);
    }

    public static <S, K, V> StateRequest<S, K, V, Void> ofValuePut(S state, K key, V value, InternalStateFuture<Void> future) {
        return new StateRequest<>(state, PUT, key, value, future);
    }


    public enum RequestType {
        GET, PUT
    }
}
