package org.apache.flink.runtime.epochmanager;

import org.apache.flink.runtime.state.async.BatchingComponent;
import org.apache.flink.util.function.RunnableWithException;

public class StrictEpochManager extends AbstractEpochManager {
    private BatchingComponent batchingComponent;

    public StrictEpochManager() {
        super();
    }

    public void setBatchingComponent(BatchingComponent batchingComponent) {
        this.batchingComponent = batchingComponent;
    }

    // Strictly-ordered Mode
    public void onNonRecord(RunnableWithException callback) throws Exception {
        assert outputQueue.size() == 1;
        Epoch lastEpoch = outputQueue.get(outputQueue.size() - 1);
        lastEpoch.callback = callback;
        lastEpoch.close();

        batchingComponent.drainAllInFlightData();
        assert lastEpoch.ongoingRecordCount == 0;
        if (lastEpoch.tryTriggerCallback() && outputQueue.size() > 0) {
            outputQueue.remove(0);
        }
        // LOG.debug("onNonRecord: {} {}", this, lastEpoch);
        Epoch epoch = new Epoch(0, callback);
        outputQueue.add(epoch);
    }

    @Override
    public String toString() {
        return String.format("StrictEpochManager@%s{outputQueue=%s}", hashCode(), outputQueue.size());
    }
}
