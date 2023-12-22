package org.apache.flink.streaming.examples.kvseparate.source;

import org.apache.commons.lang3.RandomStringUtils;

public class UniformDistributionSource extends GenerateSource {

    private final int valueLength;

    public UniformDistributionSource(int keyMaxCount, long sourceRate, int keyLength, int valueLength, int startKey) {
        super(keyMaxCount, sourceRate, keyLength, startKey);
        this.valueLength = valueLength;
    }

    @Override
    protected String generateValue() {
        return RandomStringUtils.random(valueLength);
    }

    @Override
    protected int getCurValueAvgStringLength() {
        return valueLength;
    }


}
