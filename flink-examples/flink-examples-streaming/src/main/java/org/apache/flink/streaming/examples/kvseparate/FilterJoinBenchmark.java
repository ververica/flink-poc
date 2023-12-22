package org.apache.flink.streaming.examples.kvseparate;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.examples.kvseparate.source.GaussianDistributionWithSinChangeModeSource;
import org.apache.flink.streaming.examples.kvseparate.source.GenerateSource;
import org.apache.flink.streaming.examples.kvseparate.source.JoinSuccessRatioChangeModeSource;
import org.apache.flink.streaming.examples.kvseparate.source.UniformDistributionSource;

public class FilterJoinBenchmark {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);
        Configuration configuration = JobConfig.getConfiguration(params);

        env.getConfig().setGlobalJobParameters(configuration);
        env.setParallelism(configuration.getInteger(JobConfig.PARALLELISM));
        env.disableOperatorChaining();
        String jobName = configuration.getString(JobConfig.JOB_NAME);

        JobConfig.configureCheckpoint(env, configuration);

        //JobConfig.setStateBackend(env, configuration);

        // set JVM heap size(MB)
        Tuple2<GenerateSource, GenerateSource> twoSource = getSource(configuration);
        DataStream<RowData> source1 = env.addSource(twoSource.f0)
                .setParallelism(configuration.getInteger(JobConfig.SOURCE_PARALLELISM));
        DataStream<RowData> source2 = env.addSource(twoSource.f1)
                .setParallelism(configuration.getInteger(JobConfig.SOURCE_PARALLELISM));
        source1.connect(source2).keyBy(new RowDataKeySelector(), new RowDataKeySelector())
                .transform("FilterJoin", TypeInformation.of(String.class), new JoinOperator())
                .addSink(new BlackHoleSink()).name("BlackHole Sink");

        env.execute(jobName);
    }


    static class RowDataKeySelector implements KeySelector<RowData, String> {

        @Override
        public String getKey(RowData rowData) throws Exception {
            return rowData.getKey();
        }
    }

    private static CheckpointingMode getCheckpointMode(Configuration configuration) {
        String modeString = configuration.getString(JobConfig.CHECKPOINT_MODE);
        if ("EXACTLY_ONCE".equals(modeString)) {
            return CheckpointingMode.EXACTLY_ONCE;
        } else if ("AT_LEAST_ONCE".equals(modeString)) {
            return CheckpointingMode.AT_LEAST_ONCE;
        }
        throw new RuntimeException("UnSupported checkpoint mode.");
    }

    private static Tuple2<GenerateSource, GenerateSource> getSource(Configuration configuration) {
        // configure source
        int keyNumber = configuration.getInteger(JobConfig.KEY_NUMBER);
        int keyLength = configuration.getInteger(JobConfig.KEY_LENGTH);
        int keyRate = configuration.getInteger(JobConfig.KEY_RATE);
        double keyHitRatio = configuration.getDouble(JobConfig.KEY_HIT_RATIO);
        int valueAvgLength = configuration.getInteger(JobConfig.VALUE_AVG_LENGTH);

        String distribution = configuration.getString(JobConfig.SOURCE_VALUE_LENGTH_DISTRIBUTION);
        GenerateSource.ValueLengthDistribution valueLengthDistribution = GenerateSource.ValueLengthDistribution.valueOf(distribution);

        GenerateSource source1, source2;
        if (valueLengthDistribution == GenerateSource.ValueLengthDistribution.UNIFORM) {
            source1 = new UniformDistributionSource(keyNumber, keyRate, keyLength, valueAvgLength, 0);
            source2 = new UniformDistributionSource(keyNumber, keyRate, keyLength, valueAvgLength, (int) ((1 - keyHitRatio) * keyNumber));
        } else if (valueLengthDistribution == GenerateSource.ValueLengthDistribution.JOIN_SUCCESS_RATIO_CHANGE) {
            int valueLengthStandardDeviation = configuration.getInteger(JobConfig.VALUE_LENGTH_STANDARD_DEVIATION, (int) (valueAvgLength * 0.2));
            long valueLengthChangePeriodic = configuration.getLong(JobConfig.VALUE_LENGTH_CHANGE_PERIODIC);
            source1 = new JoinSuccessRatioChangeModeSource(keyNumber, keyRate, keyLength, valueLengthStandardDeviation,
                    valueAvgLength, 0, false, keyHitRatio, valueLengthChangePeriodic);
            source2 = new JoinSuccessRatioChangeModeSource(keyNumber, keyRate, keyLength, valueLengthStandardDeviation,
                    valueAvgLength, -1, true, keyHitRatio, valueLengthChangePeriodic);
        } else if (valueLengthDistribution == GenerateSource.ValueLengthDistribution.GAUSSIAN_WITH_SIN) {
            int valueLengthStandardDeviation = configuration.getInteger(JobConfig.VALUE_LENGTH_STANDARD_DEVIATION, (int) (valueAvgLength * 0.2));
            long valueLengthChangePeriodic = configuration.getLong(JobConfig.VALUE_LENGTH_CHANGE_PERIODIC);
            source1 = new GaussianDistributionWithSinChangeModeSource(keyNumber, keyRate, keyLength, valueAvgLength,
                    0, valueLengthStandardDeviation, valueLengthChangePeriodic);
            source2 = new GaussianDistributionWithSinChangeModeSource(keyNumber, keyRate, keyLength, valueAvgLength,
                    (int) ((1 - keyHitRatio) * keyNumber), valueLengthStandardDeviation, valueLengthChangePeriodic);
        } else {
            throw new UnsupportedOperationException("Unknown value length distribution type");
        }

        return Tuple2.of(source1, source2);
    }
}
