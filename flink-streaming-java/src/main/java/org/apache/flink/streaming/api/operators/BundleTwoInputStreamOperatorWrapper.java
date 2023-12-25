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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.util.*;

/** @param <OUT> */
public class BundleTwoInputStreamOperatorWrapper<IN1, IN2, OUT> implements TwoInputStreamOperator<IN1, IN2, OUT> {

    TwoInputStreamOperator<IN1, IN2, OUT> wrapped;

    private transient BatchingContext batchingContext;

    public BundleTwoInputStreamOperatorWrapper(TwoInputStreamOperator<IN1, IN2, OUT> wrapped) {
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
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        if (batchingContext.insertIntoBatch1(element)) {
            finishBundle();
        }
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        if (batchingContext.insertIntoBatch2(element)) {
            finishBundle();
        }
    }

    private void finishBundle() throws Exception {
        Iterator<Tuple2<Boolean, StreamRecord<?>>> iterator = batchingContext.finishBundle();
        while (iterator.hasNext()) {
            Tuple2<Boolean, StreamRecord<?>> tuple = iterator.next();
            if (tuple.f0) {
                wrapped.processElement1((StreamRecord<IN1>) tuple.f1);
            } else {
                wrapped.processElement2((StreamRecord<IN2>) tuple.f1);
            }
        }
        batchingContext.clear();
    }

    @Override
    public void processWatermark1(Watermark mark) throws Exception {
        finishBundle();
        wrapped.processWatermark1(mark);
    }

    @Override
    public void processWatermark2(Watermark mark) throws Exception {
        finishBundle();
        wrapped.processWatermark2(mark);
    }

    @Override
    public void processWatermarkStatus1(WatermarkStatus watermarkStatus) throws Exception {
        wrapped.processWatermarkStatus1(watermarkStatus);
    }

    @Override
    public void processWatermarkStatus2(WatermarkStatus watermarkStatus) throws Exception {
        wrapped.processWatermarkStatus2(watermarkStatus);
    }

    @Override
    public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {
        wrapped.processLatencyMarker1(latencyMarker);
    }

    @Override
    public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {
        wrapped.processLatencyMarker2(latencyMarker);
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
        LinkedHashMap<Object, List<StreamRecord<IN1>>> keyToValueMap1;
        LinkedHashMap<Object, List<StreamRecord<IN2>>> keyToValueMap2;

        LinkedList<Boolean> nextIsList1;

        int batchingSize;

        int current = 0;

        BatchingContext(int batchingSize) {
            this.batchingSize = batchingSize;
            this.keyToValueMap1 = new LinkedHashMap<>();
            this.keyToValueMap2 = new LinkedHashMap<>();
            this.nextIsList1 = new LinkedList<>();
        }

        boolean insertIntoBatch1(StreamRecord<IN1> value) {
            keyToValueMap1.computeIfAbsent(getCurrentKey(), (k) -> {
                nextIsList1.add(true);
                return new LinkedList<>();
            }).add(value);
            return (++current) >= batchingSize;
        }

        boolean insertIntoBatch2(StreamRecord<IN2> value) {
            keyToValueMap2.computeIfAbsent(getCurrentKey(), (k) -> {
                nextIsList1.add(false);
                return new LinkedList<>();
            }).add(value);
            return (++current) >= batchingSize;
        }

        Iterator<Tuple2<Boolean, StreamRecord<?>>> finishBundle() {
           return new Iterator<Tuple2<Boolean, StreamRecord<?>>>() {

               private Collection<Object> keys1 = keyToValueMap1.keySet();
               private Collection<Object> keys2 = keyToValueMap2.keySet();

               private Iterator<Map.Entry<Object, List<StreamRecord<IN1>>>> iterator1 = keyToValueMap1.entrySet().iterator();
               private Iterator<Map.Entry<Object, List<StreamRecord<IN2>>>> iterator2 = keyToValueMap2.entrySet().iterator();

               private Iterator<?> current;

               private Iterator<Boolean> isList1Iterator = nextIsList1.iterator();

               private boolean isList1;

               @Override
               public boolean hasNext() {
                   return (current != null && current.hasNext()) || iterator1.hasNext() || iterator2.hasNext();
               }

               @Override
               public Tuple2<Boolean, StreamRecord<?>> next() {
                   if (current == null || !current.hasNext()) {
                       current = null;
                       // parse next iterator
                       if (isList1Iterator.hasNext()) {
                           isList1 = isList1Iterator.next();
                           if (isList1) {
                               setCurrentKeys(keys1);
                               Map.Entry<Object, List<StreamRecord<IN1>>> entry = iterator1.next();
                               setCurrentKey(entry.getKey());
                               current = entry.getValue().iterator();
                           } else {
                               setCurrentKeys(keys2);
                               Map.Entry<Object, List<StreamRecord<IN2>>> entry = iterator2.next();
                               setCurrentKey(entry.getKey());
                               current = entry.getValue().iterator();
                           }
                       }
                   }
                   if (current != null) {
                       return Tuple2.of(isList1, (StreamRecord<?>) current.next());
                   }
                   return null;
               }
           };
        }

        void clear() {
            keyToValueMap1.clear();
            keyToValueMap2.clear();
            nextIsList1.clear();
            current = 0;
            wrapped.clearCurrentKeysCache();
        }
    }
}
