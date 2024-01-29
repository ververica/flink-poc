package org.apache.flink.streaming.runtime.tasks;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.function.RunnableWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.LinkedList;

/**
 * Preserve the order of record and non-record inputs under the asynchronous state interface.
 * Not thread-safe, all methods should be performed in the mailbox thread.
 */
public class EpochManager {
    protected static final Logger LOG = LoggerFactory.getLogger(EpochManager.class);
    enum EpochStatus {
        /**
         * The latter non-record input has not arrived. So arriving records will be collected into this epoch.
         */
        Open,
        /**
         * The records belong to this epoch is settled since the following non-record input has arrived.
         */
        Closed,
        /**
         * The records of this epoch have finished execution after the epoch is closed.
         */
        Finished
    }

    /**
     * All inputs are segment into distinct epochs, marked by the arrival of non-record inputs.
     * Records are assigned to a unique epoch based on their arrival.
     */
    class Epoch {
        private long startId;

        private int ongoingRecordCount;

        @Nonnull
        private RunnableWithException callback;

        private EpochStatus status;

        public Epoch(long startId, int recordCount, RunnableWithException callback) {
            this.startId = startId;
            this.ongoingRecordCount = recordCount;
            this.callback = callback;
            this.status = EpochStatus.Open;
        }

        public boolean completeOneRecord() throws Exception {
            ongoingRecordCount--;
            if (ongoingRecordCount == 0 && this.status == EpochStatus.Closed) {
                LOG.trace("Epoch {} is finished", this);
                callback.run();
                return true;
            }
            return false;
        }

        public void close() {
            this.status = EpochStatus.Closed;
        }

        public String toString() {
            return String.format("Epoch{startId=%d, ongoingRecord=%d, status=%s}", startId, ongoingRecordCount, status);
        }
    }

    private long inputCount;
    private LinkedList<Epoch> outputQueue;

    public EpochManager() {
        this.inputCount = 0;
        this.outputQueue = new LinkedList<>();
        this.outputQueue.add(new Epoch(0, 0, () -> {}));
    }

    public long onRecord() {
        LOG.trace("onRecord: {}", this);
        Epoch lastEpoch = outputQueue.get(outputQueue.size() - 1);
        lastEpoch.ongoingRecordCount++;
        return inputCount++;
    }

    public long onNonRecord(RunnableWithException callback) {
        LOG.trace("onNonRecord: {}", this);
        Epoch lastEpoch = outputQueue.get(outputQueue.size() - 1);
        lastEpoch.close();
        Epoch epoch = new Epoch(inputCount, 0, callback);
        outputQueue.add(epoch);
        return inputCount++;
    }

    public void completeOneRecord(long recordId) throws Exception {
        LOG.trace("completeOneRecord: recordId={}, {}", recordId, this);
        Tuple2<Epoch, Integer> target = findEpoch(recordId);
        Epoch epoch = target.f0;
        if (epoch.completeOneRecord() && target.f1 == 0) {
            outputQueue.remove(0);
        }
    }

    private Tuple2<Epoch, Integer> findEpoch(long recordId) {
        Epoch prev = null;
        int epochIndex = 0;
        for (Epoch epoch : outputQueue) {
            if (epoch.startId > recordId) {
                break;
            }
            prev = epoch;
            epochIndex++;
        }
        return Tuple2.of(prev, epochIndex - 1);
    }

    @Override
    public String toString() {
        return String.format("EpochManager@%s{inputCount=%d, outputQueue=%s}", hashCode(), inputCount, outputQueue.size());
    }
}
