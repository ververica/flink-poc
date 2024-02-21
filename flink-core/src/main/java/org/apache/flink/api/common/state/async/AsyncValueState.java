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
import org.apache.flink.api.common.state.State;

import java.io.IOException;

/**
 * Async version corresponding to {@link org.apache.flink.api.common.state.ValueState}.
 */
@PublicEvolving
public interface AsyncValueState<T> extends AsyncState {

    StateFuture<T> value() throws StateUncheckedIOException;

    StateFuture<Void> update(T value) throws StateUncheckedIOException;

}
