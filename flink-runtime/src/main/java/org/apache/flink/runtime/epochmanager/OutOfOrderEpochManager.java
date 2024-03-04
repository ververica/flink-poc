package org.apache.flink.runtime.epochmanager;

import org.apache.flink.util.function.RunnableWithException;

/**
 * Preserve the order of record and non-record inputs under the asynchronous state interface.
 * Not thread-safe, all methods should be performed in the mailbox thread.
 */
public class OutOfOrderEpochManager extends AbstractEpochManager {
    public OutOfOrderEpochManager() {
        super();
    }

    // Out-of-order Mode
    public void onNonRecord(RunnableWithException callback) throws Exception {
        Epoch lastEpoch = outputQueue.get(outputQueue.size() - 1);
        lastEpoch.callback = callback;
        lastEpoch.close();
        if (outputQueue.size() == 1) { // which means the first epoch
            if (lastEpoch.tryTriggerCallback()) {
                outputQueue.remove(0);
            }
        }
        Epoch epoch = new Epoch(0, callback);
        outputQueue.add(epoch);
    }

    @Override
    public String toString() {
        return String.format("EpochManager@%s{outputQueue=%s}", hashCode(), outputQueue.size());
    }
}
