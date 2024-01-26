package org.apache.flink.runtime.state.async;

import org.apache.flink.api.common.operators.MailboxExecutor;

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

    private final Map<K, R> noConflictInFlightRecords = new ConcurrentHashMap<>();

    private final RecordBatchingContainer<K> batchingStateRequests = new RecordBatchingContainer<>();
    private final RecordBatchingContainer<K> pendingStateRequests = new RecordBatchingContainer<>();

    private StateExecutor<K> stateExecutor;

    private final AtomicInteger inFlightRecordNum = new AtomicInteger(0);

    private final MailboxExecutor mailboxExecutor;

    public BatchingComponent(MailboxExecutor mailboxExecutor) {
        this.mailboxExecutor = mailboxExecutor;
    }

    public <F> void processStateRequest(StateRequest<?, K, ?, F> request, RecordContext<K, R> recordContext) throws IOException {
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

        if (inFlightRecordNum.get() > 1000) {
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
        try {
            while (inFlightRecordNum.get() > 6000) {
                mailboxExecutor.yield();
            }

            recordContext.setHeldStateAccessToken();
            inFlightRecordNum.incrementAndGet();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    public void releaseStateAccessToken(R record, K key) {
        R existRecord = noConflictInFlightRecords.get(key);
        assert existRecord == record;
        LOG.trace("remove record {} key {}", record, key);
        inFlightRecordNum.decrementAndGet();
    }

    private void fireOneBatch() throws IOException {
       stateExecutor.executeBatchRequests(batchingStateRequests);
       resetBatchingStateRequestQueue();
    }

    @SuppressWarnings("unchecked")
    private void resetBatchingStateRequestQueue() {
        batchingStateRequests.clear();
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
            mailboxExecutor.yield();
        }
    }

    public <V> StateFutureImpl<R, K, V> newStateFuture(
            K currentKey,
            RecordContext<K, R> currentRecord,
            InternalKeyContext<K> internalKeyContext) {
        return new StateFutureImpl<>(currentKey, currentRecord, internalKeyContext, this::registerCallBackIntoMailBox);
    }

    private void registerCallBackIntoMailBox(RunnableWithException callBack) {
        mailboxExecutor.execute(callBack, "AsyncStateCallBack");
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
