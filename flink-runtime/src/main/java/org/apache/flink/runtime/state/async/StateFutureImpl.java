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
import org.apache.flink.api.common.state.async.StateFuture;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.util.function.RunnableWithException;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

@Internal
public class StateFutureImpl<R, K, V> implements InternalStateFuture<V> {

    private final CompletableFuture<V> future;

    private final K currentKey;

    private final RecordContext<K, R> currentRecord;

    private final InternalKeyContext<K> keyContext;

    private final Consumer<RunnableWithException> registerMailBoxCallBackFunc;

    public StateFutureImpl(K currentKey,
                           RecordContext<K, R> currentRecord,
                           InternalKeyContext<K> internalKeyContext,
                           Consumer<RunnableWithException> registerMailBoxCallBackFunc) {
        this.future = new CompletableFuture<>();
        this.currentKey = currentKey;
        this.currentRecord = currentRecord;
        this.keyContext = internalKeyContext;
        this.registerMailBoxCallBackFunc = registerMailBoxCallBackFunc;
        this.currentRecord.retain();
    }

    @Override
    public void complete(V value) {
        future.complete(value);
        currentRecord.release();
    }

    @Override
    public <C> StateFuture<C> then(Function<? super V, ? extends C> action) {
        StateFutureImpl<R, K, C> stateFuture = new StateFutureImpl<>(currentKey, currentRecord, keyContext, registerMailBoxCallBackFunc);
        future.thenAccept(value -> {
            registerMailBoxCallBackFunc.accept(() -> {
                keyContext.setCurrentKey(currentKey);
                keyContext.setCurrentRecordContext(currentRecord);
                C result = action.apply(value);
                stateFuture.complete(result);
            });
        });
        return stateFuture;
    }

    @Override
    public StateFuture<Void> then(Consumer<? super V> action) {
        StateFutureImpl<R, K, Void> stateFuture = new StateFutureImpl<>(currentKey, currentRecord, keyContext, registerMailBoxCallBackFunc);
        future.thenAccept(value -> {
            registerMailBoxCallBackFunc.accept(() -> {
                keyContext.setCurrentKey(currentKey);
                keyContext.setCurrentRecordContext(currentRecord);
                action.accept(value);
                stateFuture.complete(null);
            });
        });
        return stateFuture;
    }

    @Override
    public RecordContext<K, R> getCurrentRecordContext() {
        return currentRecord;
    }
}
