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

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The Future used for async state interface.
 */
@PublicEvolving
public interface StateFuture<T> {

    /**
     * Returns a new StateFuture that, when this future completes normally, is executed with
     * this future's result as the argument to the supplied function.
     *
     * @param action the function to use to compute the value of the returned StateFuture
     * @param <C> the function's return type
     * @return the new StateFuture
     */
    <C> StateFuture<C> then(Function<? super T, ? extends C> action);

    /**
     * Returns a new StateFuture that, when this future completes normally, is executed with this
     * future's result as the argument to the supplied action.
     *
     * @param action the action to perform before completing the returned StateFuture.
     * @return the new StateFuture
     */
    StateFuture<Void> then(Consumer<? super T> action);
}
