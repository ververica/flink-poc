package org.apache.flink.streaming.examples.kvseparate.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.apache.flink.streaming.examples.kvseparate.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Random;

public abstract class GenerateSource extends RichParallelSourceFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(GenerateSource.class);

    private transient volatile boolean isRunning;

    private final int keyMaxCount;
    private final long sourceRate;
    private final int keyLength;
    private final int startKey;
    protected ThrottledIterator<Integer> sourceKeyGenerator;
    private transient char[] fatArray;

    public GenerateSource(int keyMaxCount, long sourceRate, int keyLength, int startKey) {
        this.keyMaxCount = keyMaxCount;
        this.sourceRate = sourceRate;
        this.keyLength = keyLength;
        this.startKey = startKey;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.isRunning = true;
        Iterator<Integer> randomKeyGenerator = new RandomNumberSourceIterator(keyMaxCount, System.currentTimeMillis());
        this.sourceKeyGenerator = new ThrottledIterator<>(randomKeyGenerator, sourceRate);
        this.fatArray = new char[keyLength];
        Random random = new Random(0);
        for (int i = 0; i < fatArray.length; i++) {
            fatArray[i] = (char) random.nextInt();
        }
        getRuntimeContext().getMetricGroup().addGroup("source").gauge("avgValueStringLength", this::getCurValueAvgStringLength);
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
        while (isRunning) {
            RowData data = generateRandomData();
            sourceContext.collect(data);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }

    @Override
    public void close() {
        this.isRunning = false;
    }

    protected RowData generateRandomData() {
        int key = sourceKeyGenerator.next() + startKey;
        String value = generateValue();
        return RowData.of(covertToString(key), value);
    }

    protected abstract String generateValue();

    protected abstract int getCurValueAvgStringLength();

    protected String covertToString(int number) {
        String a = String.valueOf(number);
        StringBuilder builder = new StringBuilder(keyLength);
        builder.append(a);
        builder.append(fatArray, 0, keyLength - a.length());
        return builder.toString();
    }

    public enum ValueLengthDistribution {
        UNIFORM, JOIN_SUCCESS_RATIO_CHANGE, GAUSSIAN_WITH_SIN
    }
}
