package org.apache.flink.runtime.epochmanager;

import org.apache.flink.runtime.state.async.BatchingComponent;
import org.apache.flink.util.function.RunnableWithException;

import javax.annotation.Nonnull;
import java.util.LinkedList;

public abstract class AbstractEpochManager {

    enum ExecutionMode {
        Strictly_Ordered,
        Out_Of_Order
    }

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
    public static class Epoch {

        public static final Epoch EMPTY = new Epoch(0, () -> {
        });

        public int ongoingRecordCount;

        @Nonnull
        public RunnableWithException callback;

        public EpochStatus status;

        public Epoch(int recordCount, RunnableWithException callback) {
            this.ongoingRecordCount = recordCount;
            this.callback = callback;
            this.status = EpochStatus.Open;
        }

        public boolean tryTriggerCallback() throws Exception {
            if (ongoingRecordCount == 0 && this.status == EpochStatus.Closed) {
                callback.run();
                return true;
            }
            return false;
        }

        public void close() throws Exception {
            this.status = EpochStatus.Closed;
        }

        public String toString() {
            return String.format("Epoch{ongoingRecord=%d, status=%s}", ongoingRecordCount, status);
        }
    }

    protected LinkedList<Epoch> outputQueue;

    public AbstractEpochManager() {
        this.outputQueue = new LinkedList<>();
        this.outputQueue.add(Epoch.EMPTY);
    }

    public void setBatchingComponent(BatchingComponent batchingComponent) {
    }


    public Epoch onRecord() {
        Epoch lastEpoch = outputQueue.get(outputQueue.size() - 1);
        // LOG.trace("onRecord: {} {}", this, lastEpoch);
        lastEpoch.ongoingRecordCount++;
        return lastEpoch;
    }

    public abstract void onNonRecord(RunnableWithException callback) throws Exception;

    public void completeOneRecord(Epoch epoch) throws Exception {
        // LOG.trace("completeOneRecord: recordId={}, {}, {}", recordId, this, target);
        epoch.ongoingRecordCount--;
        if (epoch.tryTriggerCallback() && outputQueue.size() > 0) {
            if (epoch == outputQueue.getFirst()) {
                outputQueue.remove(0);
            }
        }
    }
}
