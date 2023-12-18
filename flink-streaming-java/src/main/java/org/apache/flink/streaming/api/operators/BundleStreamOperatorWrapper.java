/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/** @param <OUT> */
public class BundleStreamOperatorWrapper<IN, OUT> implements OneInputStreamOperator<IN, OUT> {

    OneInputStreamOperator<IN, OUT> wrapped;

    private transient BatchingContext batchingContext;

    public BundleStreamOperatorWrapper(OneInputStreamOperator<IN, OUT> wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        wrapped.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        wrapped.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        if (batchingContext.insertIntoBatch(element)) {
            finishBundle();
        }
    }

    private void finishBundle() throws Exception {
        batchingContext.finishBundle();
        for (Map.Entry<Object, List<StreamRecord<IN>>> entry :
                batchingContext.keyToValueMap.entrySet()) {
            setCurrentKey(entry.getKey());
            for (StreamRecord<IN> record : entry.getValue()) {
                wrapped.processElement(record);
            }
        }
        batchingContext.clear();
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        finishBundle();
        wrapped.processWatermark(mark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        wrapped.processWatermarkStatus(watermarkStatus);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        wrapped.processLatencyMarker(latencyMarker);
    }

    @Override
    public void setCurrentKey(Object key) {
        wrapped.setCurrentKey(key);
    }

    @Override
    public Object getCurrentKey() {
        return wrapped.getCurrentKey();
    }

    @Override
    public void setCurrentKeys(Collection<?> keys) {
        wrapped.setCurrentKeys(keys);
    }

    @Override
    public Collection<?> getCurrentKeys() {
        return getCurrentKeys();
    }

    @Override
    public void setKeyContextElement(StreamRecord<IN> record) throws Exception {
        wrapped.setKeyContextElement(record);
    }

    @Override
    public void open() throws Exception {
        batchingContext = new BatchingContext(1000);
        wrapped.open();
    }

    @Override
    public void finish() throws Exception {
        finishBundle();
        wrapped.finish();
    }

    @Override
    public void close() throws Exception {
        wrapped.close();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        wrapped.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation)
            throws Exception {
        finishBundle();
        return wrapped.snapshotState(checkpointId, timestamp, checkpointOptions, storageLocation);
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
            throws Exception {
        wrapped.initializeState(streamTaskStateManager);
    }

    @Override
    public void setKeyContextElement1(StreamRecord<?> record) throws Exception {
        wrapped.setKeyContextElement1(record);
    }

    @Override
    public void setKeyContextElement2(StreamRecord<?> record) throws Exception {
        wrapped.setKeyContextElement2(record);
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return wrapped.getMetricGroup();
    }

    @Override
    public OperatorID getOperatorID() {
        return wrapped.getOperatorID();
    }

    private class BatchingContext {
        LinkedHashMap<Object, List<StreamRecord<IN>>> keyToValueMap;

        int batchingSize;

        int current = 0;

        BatchingContext(int batchingSize) {
            this.batchingSize = batchingSize;
            this.keyToValueMap = new LinkedHashMap<>();
        }

        boolean insertIntoBatch(StreamRecord<IN> value) {
            keyToValueMap.computeIfAbsent(getCurrentKey(), (k) -> new LinkedList<>()).add(value);
            return (++current) >= batchingSize;
        }

        void finishBundle() {
            setCurrentKeys(new ArrayList<>(keyToValueMap.keySet()));
        }

        void clear() {
            keyToValueMap.clear();
            current = 0;
        }
    }
}
