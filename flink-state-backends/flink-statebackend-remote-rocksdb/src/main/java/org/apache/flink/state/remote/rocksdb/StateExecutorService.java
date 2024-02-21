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

package org.apache.flink.state.remote.rocksdb;

import org.apache.flink.api.common.state.async.StateUncheckedIOException;
import org.apache.flink.runtime.state.async.StateExecutor;
import org.apache.flink.runtime.state.async.StateRequest;
import org.apache.flink.state.remote.rocksdb.internal.RemoteRocksdbValueState;
import org.apache.flink.state.remote.rocksdb.internal.RemoteState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StateExecutorService<K> implements StateExecutor<K> {

    private static final Logger LOG = LoggerFactory.getLogger(StateExecutorService.class);
    
    private final ExecutorService stateIOExecutors;

    private final Executor coordinatorExecurtor;

    private final Set<RemoteState> registeredStates = new HashSet<>();
    
    public StateExecutorService(int ioParallelism) {
        this.stateIOExecutors = Executors.newFixedThreadPool(ioParallelism);
        this.coordinatorExecurtor = Executors.newSingleThreadScheduledExecutor();
    }

    public synchronized <S extends RemoteState> void registerState(S state) {
        registeredStates.add(state);
    }

    @Override
    public CompletableFuture<Boolean> executeBatchRequests(Iterable<StateRequest<?, K, ?, ?>> stateRequests) {
        Map<RemoteState, Map<StateRequest.RequestType, List<StateRequest<?, K, ?, ?>>>> requestClassifier = new HashMap<>();
        int stateRequestCount = 0;
        for (StateRequest<?, K, ?, ?> request : stateRequests) {
            Map<StateRequest.RequestType, List<StateRequest<?, K, ?, ?>>> sameStateRequests =
                    requestClassifier.computeIfAbsent((RemoteState) request.getState(), (key) -> new HashMap<>());

            List<StateRequest<?, K, ?, ?>> sameTypeRequests =
                    sameStateRequests.computeIfAbsent(request.getQuestType(), (key) -> new ArrayList<>());
            sameTypeRequests.add(request);
            stateRequestCount++;
        }

        LOG.debug("Submit state batch requests: count {}", stateRequestCount);
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            long startTime = System.currentTimeMillis();
            for (Map.Entry<RemoteState, Map<StateRequest.RequestType, List<StateRequest<?, K, ?, ?>>>>
                    entry : requestClassifier.entrySet()) {
                if (!registeredStates.contains(entry.getKey())) {
                    throw new FlinkRuntimeException("unregistered state");
                }
                if (entry.getKey() instanceof RemoteRocksdbValueState) {
                    processValueStateRequest((RemoteRocksdbValueState<K, ?, ?>) entry.getKey(), entry.getValue());
                } else {
                    throw new UnsupportedOperationException("Unsupported state type");
                }
            }
            future.complete(true);
            LOG.debug("Finish batch request: consumeTime {} ms.", System.currentTimeMillis() - startTime);
        }, coordinatorExecurtor);
        
        return future;
    }

    private <N, V> void processValueStateRequest(RemoteRocksdbValueState<K, N, V> valueState,
                                                 Map<StateRequest.RequestType, List<StateRequest<?, K, ?, ?>>> requests) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Map.Entry<StateRequest.RequestType, List<StateRequest<?, K, ?, ?>>> entry : requests.entrySet()) {
            if (entry.getKey() == StateRequest.RequestType.GET) {
                processValueStateGetRequests(valueState, entry.getValue(), futures);
            } else if (entry.getKey() == StateRequest.RequestType.PUT) {
                processValueStatePutRequests(valueState, entry.getValue(), futures);
            } else {
                throw new UnsupportedOperationException("Unsupported request type");
            }
        }
        try {
            FutureUtils.completeAll(futures).get();
        } catch (InterruptedException | ExecutionException | CompletionException e) {
            throw new RuntimeException(e);
        }
    }

    private <N, V> void processValueStateGetRequests(RemoteRocksdbValueState<K, N, V> valueState,
            List<StateRequest<?, K, ?, ?>> valueGetRequests, List<CompletableFuture<Void>> futures) {
        //TODO Optimize performance with the multiGet interface
        for (StateRequest<?, K, ?, ?> getRequest : valueGetRequests) {
            StateRequest<?, K, Void, V> request = (StateRequest<?, K, Void, V>) getRequest;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    V value = valueState.get(request.getKey());
                    request.getFuture().complete(value);
                } catch (IOException e) {
                    throw new StateUncheckedIOException(e);
                }
            }, stateIOExecutors));
        }
    }

    private <N, V> void processValueStatePutRequests(RemoteRocksdbValueState<K, N, V> valueState,
            List<StateRequest<?, K, ?, ?>> valueGetRequests, List<CompletableFuture<Void>> futures) {
        futures.add(CompletableFuture.runAsync(() -> {
            for (StateRequest<?, K, ?, ?> putRequest : valueGetRequests) {
                StateRequest<?, K, V, Void> request = (StateRequest<?, K, V, Void>) putRequest;
                try {
                    valueState.put(request.getKey(), request.getValue());
                    request.getFuture().complete(null);
                } catch (IOException e) {
                    throw new StateUncheckedIOException(e);
                }
            }
        }, stateIOExecutors));
    }
}
