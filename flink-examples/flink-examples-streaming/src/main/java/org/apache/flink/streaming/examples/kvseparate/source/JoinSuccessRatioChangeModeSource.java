package org.apache.flink.streaming.examples.kvseparate.source;

import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.examples.kvseparate.RowData;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class JoinSuccessRatioChangeModeSource extends GenerateSource {

    private static final Logger LOG = LoggerFactory.getLogger(JoinSuccessRatioChangeModeSource.class);

    private int startKey;
    private final int valueStringAvgLen;
    private final int valueLengthStandardDeviation;
    private final int keyTotalNum;
    private transient Random random;
    private final boolean changeKeyRange;
    private final long changePeriodic;
    private final long startTime;
    private double keyHitRatio;
    private transient Timer timer;

    public JoinSuccessRatioChangeModeSource(int keyMaxCount, long sourceRate, int keyLength, int valueLengthStandardDeviation,
                                            int valueLength, int startKey, boolean changeKeyHitRatio, double keyHitRatio, long changePeriodic) {
        super(keyMaxCount, sourceRate, keyLength, startKey);
        this.changeKeyRange = changeKeyHitRatio;
        this.changePeriodic = changePeriodic;
        this.valueStringAvgLen = valueLength;
        this.valueLengthStandardDeviation = valueLengthStandardDeviation;
        this.keyHitRatio = keyHitRatio;
        this.keyTotalNum = keyMaxCount;
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.random = new Random(startTime);
        if (changeKeyRange) {
            startKey = (int) ((1 - keyHitRatio) * keyTotalNum);
            this.timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    long t = System.currentTimeMillis() - startTime;
                    double w = (2 * Math.PI) / changePeriodic;
                    keyHitRatio = Math.max(0.5 * Math.sin(w * t) + 0.5, 0.05D);
                    startKey = (int) ((1 - keyHitRatio) * keyTotalNum);
                    LOG.info("Current key hit ratio {}", keyHitRatio);
                }
            }, 3 * 1000, 3 * 1000);
        }

        getRuntimeContext().getMetricGroup().addGroup("source").gauge("keyHitRatio", () -> keyHitRatio);
        LOG.info("Open the JoinSuccessRatioChangeModeSource with changeKeyRange {}, changePeriodic {}, valueStringAvgLen {}, " +
                        "valueLengthStandardDeviation {}, keyHitRatio {}",
                changeKeyRange, changePeriodic, valueStringAvgLen, valueLengthStandardDeviation, keyHitRatio);
    }

    @Override
    protected String generateValue() {
        int length = (int) (valueLengthStandardDeviation * random.nextGaussian() + valueStringAvgLen);
        return RandomStringUtils.random(Math.max(length, 5));
    }



    @Override
    protected RowData generateRandomData() {
        int key = sourceKeyGenerator.next() + startKey;
        String value = generateValue();
        return RowData.of(covertToString(key), value);
    }

    @Override
    protected int getCurValueAvgStringLength() {
        return valueStringAvgLen;
    }
}
