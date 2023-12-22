package org.apache.flink.streaming.examples.kvseparate.source;

import org.apache.flink.configuration.Configuration;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class GaussianDistributionWithSinChangeModeSource extends GenerateSource {

    private static final Logger LOG = LoggerFactory.getLogger(GaussianDistributionWithSinChangeModeSource.class);

    private final int originValueAvgLength;
    private volatile int curValueAvgLength;
    private transient Random random;
    private final int valueLengthStandardDeviation;
    private final long startTime;
    private final long valueLengthChangePeriodic;

    private transient Timer timer;

    public GaussianDistributionWithSinChangeModeSource(
            int keyMaxCount,
            long sourceRate,
            int keyLength,
            int valueLength,
            int startKey,
            int valueLengthStandardDeviation,
            long valueLengthChangePeriodic) {
        super(keyMaxCount, sourceRate, keyLength, startKey);
        this.originValueAvgLength = valueLength;
        this.valueLengthStandardDeviation = valueLengthStandardDeviation;
        this.valueLengthChangePeriodic = valueLengthChangePeriodic;
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.random = new Random(System.currentTimeMillis());
        this.timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                curValueAvgLength = computeValueLength(originValueAvgLength);
                LOG.info("Current value average length {}", curValueAvgLength);
            }
        }, 3 * 1000, 3 * 1000);

        LOG.info("Open the GaussianDistributionWithSinChangeModeSource with originValueAvgLength {}, valueLengthStandardDeviation {}, valueLengthChangePeriodic {}",
                originValueAvgLength, valueLengthStandardDeviation, valueLengthChangePeriodic);
    }

    @Override
    public void close() {
        if (timer != null) {
            timer.cancel();
        }
        super.close();
    }

    @Override
    protected String generateValue() {
        int u = curValueAvgLength;
        int length = (int) (valueLengthStandardDeviation * random.nextGaussian() + u);
        return RandomStringUtils.random(Math.max(length, 5));
    }

    @Override
    protected int getCurValueAvgStringLength() {
        return curValueAvgLength;
    }

    private int computeValueLength(int valueOriginLength) {
        long t = System.currentTimeMillis() - startTime;
        double w = (2 * Math.PI) / valueLengthChangePeriodic;
        return (int) ((valueOriginLength * 0.5) * Math.sin(w * t) + valueOriginLength);
    }
}
