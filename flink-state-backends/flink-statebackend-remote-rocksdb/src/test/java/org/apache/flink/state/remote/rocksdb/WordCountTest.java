package org.apache.flink.state.remote.rocksdb;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.apache.flink.util.Collector;

import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;

import static org.apache.flink.state.remote.rocksdb.RemoteRocksDBOptions.REMOTE_ROCKSDB_MODE;
import static org.apache.flink.state.remote.rocksdb.RemoteRocksDBOptions.REMOTE_ROCKSDB_WORKING_DIR;

public class WordCountTest {

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    private static Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("1m"));
        config.set(StateBackendOptions.STATE_BACKEND,
                "org.apache.flink.state.remote.rocksdb.RemoteRocksDBStateBackendFactory");
        config.set(REMOTE_ROCKSDB_MODE, RemoteRocksDBOptions.RemoteRocksDBMode.REMOTE);
        config.set(REMOTE_ROCKSDB_WORKING_DIR, "hdfs://localhost:9000");
//        config.set(REMOTE_ROCKSDB_MODE, RemoteRocksDBOptions.RemoteRocksDBMode.LOCAL);
//        config.set(REMOTE_ROCKSDB_WORKING_DIR, "/tmp");
        return config;
    }

    @Test
    public void testWordCount() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> source = WordSource.getSource(env, 1000, 1000, 50).setParallelism(1);
        DataStream<Long> mapper = source.keyBy(e -> e).flatMap(new MixedFlatMapper()).setParallelism(1);
        mapper.print().setParallelism(1);
        env.execute();
    }

    public static class MixedFlatMapper extends RichFlatMapFunction<String, Long> {

        private transient ValueState<Integer> wordCounter;

        public MixedFlatMapper() {
        }

        @Override
        public void flatMap(String in, Collector<Long> out) throws IOException {
            Integer currentValue = wordCounter.value();

            if (currentValue != null) {
                wordCounter.update(currentValue + 1);
                out.collect(currentValue + 1L);
            } else {
                wordCounter.update(1);
                out.collect(1L);
            }
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>(
                            "wc",
                            TypeInformation.of(new TypeHint<Integer>(){}));
            wordCounter = getRuntimeContext().getState(descriptor);
        }
    }
}
