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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.epochmanager.AbstractEpochManager.Epoch;

import java.util.function.Consumer;

public class RecordContext<K, R> extends ReferenceCounted {

    private final R record;

    private final K key;

    private Epoch epoch;

    private boolean heldStateAccessToken;

    private final BatchingComponent<R, K> batchingComponentHandle;

    private final Consumer<Epoch> recordCallback;

    @VisibleForTesting
    public RecordContext(R record, K key, BatchingComponent<R, K> batchingComponent) {
        super(0);
        this.record = record;
        this.key = key;
        this.heldStateAccessToken = false;
        this.batchingComponentHandle = batchingComponent;
        this.recordCallback = ignore -> {};
    }

    public RecordContext(R record, K key, Epoch epoch, BatchingComponent<R, K> batchingComponentHandle, Consumer<Epoch> recordCallback) {
        super(0);
        this.record = record;
        this.key = key;
        this.epoch = epoch;
        this.heldStateAccessToken = false;
        this.batchingComponentHandle = batchingComponentHandle;
        this.recordCallback = recordCallback;
    }

    public static <KEY> RecordContext ofTimer(KEY key, BatchingComponent batchingComponent) {
        return new RecordContext<>(null, key, Epoch.EMPTY, batchingComponent, null);
    }

    public R getRecord() {
        return record;
    }

    public boolean heldStateAccessToken() {
        return heldStateAccessToken;
    }

    public void setHeldStateAccessToken() {
        heldStateAccessToken = true;
    }

    @Override
    protected void referenceCountReachedZero() {
        if (batchingComponentHandle!= null) {
            batchingComponentHandle.releaseStateAccessToken(record, key);
            recordCallback.accept(epoch);
        }
    }
}
