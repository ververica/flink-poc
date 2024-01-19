package org.apache.flink.runtime.state.async;

import org.apache.flink.api.common.state.async.StateFuture;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.batch.InternalBatchValueState;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class BatchKeyProcessor<K, N, V> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchKeyProcessor.class);

    private final Set<K> batchingKeys = Sets.newConcurrentHashSet();
    private final RecordBatchingContainer<K> batchingOperations = new RecordBatchingContainer<>();
    private final RecordBatchingContainer<K> pendingOperations = new RecordBatchingContainer<>();

    private final InternalBatchValueState<K, N, V> batchValueState;

    private static final int BATCH_MAX_SIZE = 1000;
    private final AtomicLong onFlyingIORequestNum = new AtomicLong(0);

    private static final long MAX_ON_FLYING_RECORDING_NUM = 10000;

    protected final ExecutorService asyncExecutors;

    private final Consumer<RunnableWithException> registerCallBackFunc;

    private final InternalKeyContext<K> keyContext;

    public BatchKeyProcessor(
            InternalBatchValueState<K, N, V> batchValueState,
            InternalKeyContext<K> keyContext,
            Consumer<RunnableWithException> registerCallBackFunc) {
        this.asyncExecutors = Executors.newFixedThreadPool(2);
        this.batchValueState = batchValueState;
        this.keyContext = keyContext;
        this.registerCallBackFunc = registerCallBackFunc;
    }

    public StateFuture<V> get(K key) throws IOException {
        LOG.trace("Async get {} from BatchKeyProcessor", key);
        StateFutureImpl<K, V> stateFuture = new StateFutureImpl<>(key, keyContext, registerCallBackFunc);
        Operation<K, V, V> getOperation = Operation.ofGet(stateFuture);
        handleOneStateOperation(key, getOperation);
        return stateFuture;
    }

    public StateFuture<Void> put(K key, V value) throws IOException {
        LOG.trace("Async put {} to BatchKeyProcessor", key);
        StateFutureImpl<K, Void> stateFuture = new StateFutureImpl<>(key, keyContext, registerCallBackFunc);
        Operation<K, V, Void> putOperation = Operation.ofPut(value, stateFuture);
        handleOneStateOperation(key, putOperation);
        return stateFuture;
    }

    private void handleOneStateOperation(K key, Operation<K, V, ?> stateOperation) throws IOException {
        boolean keyConflict = keyConflict(key);
        if (keyConflict) {
            pendingOperations.add(Tuple2.of(key, stateOperation));
            if (pendingOperations.size() > BATCH_MAX_SIZE) {
                backPressureBecauseTooManyPendingOperations();
            }
            return;
        }

        batchingOperations.offer(Tuple2.of(key, stateOperation));
        batchingKeys.add(key);

        if (batchingOperations.size() + pendingOperations.size() > BATCH_MAX_SIZE) {
            LOG.trace("handle one batch: keyConflict {}, pendingOperations size {}", keyConflict, batchingOperations.size());
            fireOneBatch(!keyContext.isInCallBackProcess());
        }
        return;
    }

    public void endKeyProcess(K key) {
        boolean result =  batchingKeys.remove(key);
        LOG.trace("remove key {}", key);
        assert result;
    }

    private boolean keyConflict(K key) {
        return !keyContext.isInCallBackProcess() && batchingKeys.contains(key);
    }

    private void fireOneBatch(boolean allowBackpressure) throws IOException {
        if (allowBackpressure && onFlyingIORequestNum.get() > MAX_ON_FLYING_RECORDING_NUM) {
            backPressureBecauseTooManyOnFlyingIORequests();
        }

        if (batchingOperations.isEmpty()) {
            resetBatchingOperationQueue();
            return;
        }
        
        Map<K, Operation<K, V, V>> getOperations = new HashMap<>();
        Map<K, Operation<K, V, Void>> putOperations = new HashMap<>();
        for (Tuple2<K, Operation<K, ?, ?>> entry : batchingOperations) {
            if (entry.f1.type == OperationType.PUT) {
                putOperations.put(entry.f0, (Operation<K, V, Void>) entry.f1);
            } else {
                getOperations.put(entry.f0, (Operation<K, V, V>) entry.f1);
            }
            onFlyingIORequestNum.incrementAndGet();
        }
        LOG.trace("Submit state task: onFlyingRecordNum {}", onFlyingIORequestNum.get());
        asyncExecutors.execute(() -> {
            try {
                for (Map.Entry<K, Operation<K, V, Void>> entry : putOperations.entrySet()) {
                    batchValueState.update(entry.getKey(), entry.getValue().value);
                    onFlyingIORequestNum.decrementAndGet();
                    entry.getValue().stateFuture.complete(null);
                    LOG.trace("handle put operation for key {}, onFlyingRecordNum {}", entry.getKey(), onFlyingIORequestNum.get());
                }

                Iterable<Tuple2<K, V>> result = batchValueState.values(getOperations.keySet());
                onFlyingIORequestNum.addAndGet(-getOperations.size());
                Iterator<Tuple2<K, V>> it = result.iterator();
                while (it.hasNext()) {
                    Tuple2<K, V> kvPair = it.next();
                    Operation<K, V, V> getOperation = getOperations.get(kvPair.f0);
                    LOG.trace("handle get operation for key {}, onFlyingRecordNum {}", kvPair.f0, onFlyingIORequestNum.get());
                    getOperation.stateFuture.complete(kvPair.f1);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        resetBatchingOperationQueue();
    }

    private void resetBatchingOperationQueue() {
        batchingOperations.clear();
        Iterator<Tuple2<K, Operation<K, ?, ?>>> pendingIter = pendingOperations.iterator();
        while(pendingIter.hasNext()) {
            Tuple2<K, Operation<K, ?, ?>> keyAndOperation = pendingIter.next();
            if (!batchingKeys.contains(keyAndOperation.f0)) {
                batchingOperations.offer(keyAndOperation);
                batchingKeys.add(keyAndOperation.f0);
                pendingIter.remove();
            }
        }
    }

    private void backPressureBecauseTooManyPendingOperations() throws IOException {
        registerCallBackFunc.accept(() -> {
            if (pendingOperations.size() > BATCH_MAX_SIZE) {
                fireOneBatch(false);
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                LOG.trace("pending process by BatchKeyProcessor : pendingOperations num {}", pendingOperations.size());
                backPressureBecauseTooManyPendingOperations();
            }
        });
    }

    private void backPressureBecauseTooManyOnFlyingIORequests() {
        registerCallBackFunc.accept(() -> {
            while (onFlyingIORequestNum.get() > MAX_ON_FLYING_RECORDING_NUM) {
                Thread.sleep(50);
                LOG.info("handleOneBatch sleep 50 ms with onFlyingRecordNum {}", onFlyingIORequestNum.get());
            }
        });
    }

    static class Operation<K, V, F> {
        OperationType type;

        @Nullable V value;

        StateFutureImpl<K, F> stateFuture;

        public Operation(OperationType type,  @Nullable V value,  StateFutureImpl<K, F> stateFuture) {
            this.type = type;
            this.value = value;
            this.stateFuture = stateFuture;
        }

        public static <K, V> Operation<K, V, V> ofGet(StateFutureImpl<K, V> stateFuture) {
            return new Operation<>(OperationType.GET, null, stateFuture);
        }

        public static <K, V> Operation<K, V, Void> ofPut(V value, StateFutureImpl<K, Void> stateFuture) {
            return new Operation<>(OperationType.PUT, value, stateFuture);
        }
    }

    enum OperationType {
        GET, PUT
    }

    static class RecordBatchingContainer<K> implements Iterable<Tuple2<K, Operation<K, ?, ?>>> {
        private  ConcurrentLinkedQueue<Tuple2<K, Operation<K, ?, ?>>> batchingOperations;

        private int size = 0;

        public RecordBatchingContainer() {
            this.batchingOperations = new ConcurrentLinkedQueue<>();
        }

        public void offer(Tuple2<K, Operation<K, ?, ?>> record) {
            batchingOperations.offer(record);
            size++;
        }

        public void add(Tuple2<K, Operation<K, ?, ?>> record) {
            offer(record);
        }

        public int size() {
            return size;
        }

        public boolean isEmpty() {
            return batchingOperations.isEmpty();
        }

        public void clear() {
            batchingOperations.clear();
            size = 0;
        }

        @Override
        public Iterator<Tuple2<K, Operation<K, ?, ?>>> iterator() {
            return batchingOperations.iterator();
        }
    }
}
