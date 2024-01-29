package org.apache.flink.runtime.state.async;

import org.apache.flink.api.common.operators.MailboxExecutor;

import org.apache.flink.api.common.state.async.StateUncheckedIOException;
import org.apache.flink.runtime.state.heap.InternalKeyContext;

import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchingComponent<R, K> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchingComponent.class);

    public static final int DEFAULT_BATCH_SIZE = 1000;

    public static final int DEFAULT_MAX_IN_FLIGHT_RECORD_NUM = 6000;

    private final int batchSize;

    private final int maxInFlightRecordNum;

    private final Map<K, R> noConflictInFlightRecords = new ConcurrentHashMap<>();

    private final RecordBatchingContainer<K> batchingStateRequests = new RecordBatchingContainer<>();
    private final RecordBatchingContainer<K> pendingStateRequests = new RecordBatchingContainer<>();

    private StateExecutor<K> stateExecutor;

    private final AtomicInteger inFlightRecordNum = new AtomicInteger(0);

    private final MailboxExecutor mailboxExecutor;

    public BatchingComponent(MailboxExecutor mailboxExecutor) {
        this(mailboxExecutor, DEFAULT_BATCH_SIZE, DEFAULT_MAX_IN_FLIGHT_RECORD_NUM);
    }

    public BatchingComponent(MailboxExecutor mailboxExecutor, int batchSize, int maxInFlightRecords) {
        this.mailboxExecutor = mailboxExecutor;
        this.batchSize = batchSize;
        this.maxInFlightRecordNum = maxInFlightRecords;
        LOG.info("Create BatchingComponent: batchSize {}, maxInFlightRecordsNum {}", batchSize, maxInFlightRecords);
    }

    public <F> void processStateRequest(StateRequest<?, K, ?, F> request, RecordContext<K, R> recordContext) throws IOException {
        LOG.trace("start process request {}, {}", request.getQuestType(), request.getKey());
        if (!recordContext.heldStateAccessToken()) {
            requestStateAccessTokenUntilSuccess(recordContext);
        }

        boolean keyConflict = checkKeyConflict(request.key, recordContext.getRecord());

        if (keyConflict) {
            pendingStateRequests.offer(request);
        } else {
            noConflictInFlightRecords.put(request.key, recordContext.getRecord());
            batchingStateRequests.offer(request);
        }

        if (batchingStateRequests.size() > batchSize) {
            fireOneBatch();
        }
    }

    private boolean checkKeyConflict(K key, R record) {
        if (noConflictInFlightRecords.containsKey(key)) {
            return noConflictInFlightRecords.get(key) != record;
        }
        return false;
    }

    private void requestStateAccessTokenUntilSuccess(RecordContext<K, R> recordContext) throws IOException {
        LOG.trace("Request StateAccess Token Until Success {} {}", recordContext.getRecord(), inFlightRecordNum.get());
        try {
            while (inFlightRecordNum.get() > maxInFlightRecordNum) {
                if (!mailboxExecutor.tryYield()) {
                    fireOneBatch();
                    Thread.sleep(50);
                }
            }

            recordContext.setHeldStateAccessToken();
            inFlightRecordNum.incrementAndGet();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    public void releaseStateAccessToken(R record, K key) {

        R existRecord = noConflictInFlightRecords.remove(key);
        assert existRecord == record;
        LOG.trace("remove record {} key {}", record, key);
        inFlightRecordNum.decrementAndGet();
    }

    private void fireOneBatch() {
        try {
            tryPollPendingRequestsToBatchingQueue();
            LOG.trace("fire one batch: inFightRecordNum {}, batchingRequests num {}, pendingRequest num {}",
                    inFlightRecordNum.get(), batchingStateRequests.size(), pendingStateRequests.size());
            if (batchingStateRequests.isEmpty()) {
                return;
            }

            stateExecutor.executeBatchRequests(batchingStateRequests);
            batchingStateRequests.clear();
            tryPollPendingRequestsToBatchingQueue();
        } catch (Exception e) {
            throw new StateUncheckedIOException(new IOException(e));
        }
    }

    @SuppressWarnings("unchecked")
    private synchronized void tryPollPendingRequestsToBatchingQueue() {
        Iterator<StateRequest<?, K, ?, ?>> pendingIter = pendingStateRequests.iterator();
        while(pendingIter.hasNext()) {
            StateRequest<?, K, ?, ?> stateRequest = pendingIter.next();
            R record = (R) stateRequest.stateFuture.getCurrentRecordContext().getRecord();
            if (!checkKeyConflict(stateRequest.key, record)) {
                batchingStateRequests.offer(stateRequest);
                noConflictInFlightRecords.put(stateRequest.key, record);
                pendingIter.remove();
            }
        }
    }

    public void drainAllInFlightDataBeforeStateSnapshot() throws InterruptedException {
        while (inFlightRecordNum.get() > 0) {
            if (!mailboxExecutor.tryYield()) {
                fireOneBatch();
                Thread.sleep(50);
            }
        }
    }

    public <V> StateFutureImpl<R, K, V> newStateFuture(
            K currentKey,
            RecordContext<K, R> currentRecord,
            InternalKeyContext<K> internalKeyContext) {
        return new StateFutureImpl<>(currentKey, currentRecord, internalKeyContext, this::registerCallBackIntoMailBox);
    }

    private void registerCallBackIntoMailBox(RunnableWithException callBack) {
        LOG.trace("Register callback to MailBox");
        mailboxExecutor.execute(callBack, "AsyncStateCallBack");
    }

    public void setStateExecutor(StateExecutor<K> stateExecutor) {
        this.stateExecutor = stateExecutor;
    }

    static class RecordBatchingContainer<K> implements Iterable<StateRequest<?, K, ?, ?>> {
        private ConcurrentLinkedQueue<StateRequest<?, K, ?, ?>> batchingRequests;

        private int size = 0;

        public RecordBatchingContainer() {
            this.batchingRequests = new ConcurrentLinkedQueue<>();
        }

        public void offer(StateRequest<?, K, ?, ?> request) {
            batchingRequests.offer(request);
            size++;
            synchronized (this) {
                notify();
            }
        }

        public void add(StateRequest<?, K, ?, ?> request) {
            offer(request);
        }

        public int size() {
            return size;
        }

        public boolean isEmpty() {
            return batchingRequests.isEmpty();
        }

        public void clear() {
            batchingRequests.clear();
            size = 0;
        }

        @Override
        public Iterator<StateRequest<?, K, ?, ?>> iterator() {
            return batchingRequests.iterator();
        }
    }
}
