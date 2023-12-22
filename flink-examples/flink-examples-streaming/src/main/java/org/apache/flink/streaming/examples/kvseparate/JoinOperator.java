package org.apache.flink.streaming.examples.kvseparate;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class JoinOperator extends AbstractStreamOperator<String> implements TwoInputStreamOperator<RowData, RowData, String>{

    private static final long serialVersionUID = -3135772379944924528L;

    private transient ValueState<String> leftState;
    private transient ValueState<String> rightState;

    public JoinOperator() {
    }

    @Override
    public void open() throws Exception {
        ValueStateDescriptor<String> leftStateDescriptor =
                new ValueStateDescriptor<>(
                        "leftBucket",
                        TypeInformation.of(new TypeHint<String>() {
                        }));
        ValueStateDescriptor<String> rightStateDescriptor =
                new ValueStateDescriptor<>(
                        "rightBucket",
                        TypeInformation.of(new TypeHint<String>() {
                        }));
        this.leftState = getRuntimeContext().getState(leftStateDescriptor);
        this.rightState = getRuntimeContext().getState(rightStateDescriptor);
    }

    @Override
    public void processElement1(StreamRecord<RowData> streamRecord) throws Exception {
        processElement(streamRecord, leftState, rightState);
    }

    @Override
    public void processElement2(StreamRecord<RowData> streamRecord) throws Exception {
        processElement(streamRecord, rightState, leftState);
    }

    private void processElement(StreamRecord<RowData> input, ValueState<String> inputSideState, ValueState<String> otherSideState) throws Exception {
        inputSideState.update(input.getValue().getValue());

        String otherSideValue = otherSideState.value();
        if (otherSideValue != null) {
//            outRow.replace(otherSideValue, System.currentTimeMillis());
            output.collect(input.replace(otherSideValue));
        }
    }
}

