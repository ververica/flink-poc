package org.apache.flink.state.forst;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.async.AsyncValueState;
import org.apache.flink.api.common.state.async.AsyncValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.testutils.oss.OSSTestCredentials;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.io.IOException;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.INCREMENTAL_CHECKPOINTS;

public class WordCountTest {

//    @ClassRule
//    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
//            new MiniClusterWithClientResource(
//                    new MiniClusterResourceConfiguration.Builder()
//                            .setConfiguration(getCommonConfiguration())
//                            .setNumberTaskManagers(1)
//                            .setNumberSlotsPerTaskManager(1)
//                            .build());

    private static Configuration getCommonConfiguration() {
        Configuration config = new Configuration();
        config.set(INCREMENTAL_CHECKPOINTS, true);

        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("128m"));
        config.set(RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE, MemorySize.parse("1m"));
        config.set(StateBackendOptions.STATE_BACKEND,
                "org.apache.flink.state.remote.rocksdb.RemoteRocksDBStateBackendFactory");
        config.set(RocksDBOptions.LOCAL_DIRECTORIES, "/tmp/local-dir");
        return config;
    }

    @Test
    public void testWordCountWithLocal() throws Exception {
        Configuration config = getCommonConfiguration();
        config.set(ForStOptions.FOR_ST_WORKING_DIR, "file:///tmp/tmp-test-remote");
        config.set(ForStOptions.FOR_ST_MODE, ForStOptions.ForStMode.LOCAL);
        FileSystem.initialize(config, null);
        config.set(CHECKPOINTS_DIRECTORY, "file:///tmp/checkpoint");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.enableCheckpointing(500);
        DataStream<String> source = WordSource.getSource(env, 1000, 10, 50).setParallelism(1);
        DataStream<Long> mapper = source.keyBy(e -> e).flatMap(new MixedFlatMapper()).setParallelism(1);
        mapper.print().setParallelism(1);
        env.execute();
    }

    @Test
    public void testWordCountWithOSS() throws Exception {
        Configuration config = getCommonConfiguration();
        OSSTestCredentials.assumeCredentialsAvailable();
        config.setString("fs.oss.endpoint", OSSTestCredentials.getOSSEndpoint());
        config.setString("fs.oss.accessKeyId", OSSTestCredentials.getOSSAccessKey());
        config.setString("fs.oss.accessKeySecret", OSSTestCredentials.getOSSSecretKey());
        config.set(CHECKPOINTS_DIRECTORY, "oss://state-bj/checkpoint-remote-wc");
        config.set(ForStOptions.FOR_ST_MODE, ForStOptions.ForStMode.REMOTE);
        config.set(ForStOptions.FOR_ST_WORKING_DIR, "oss://state-bj/");
        FileSystem.initialize(config, null);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.enableCheckpointing(500);
        DataStream<String> source = WordSource.getSource(env, 20, 100, 50).setParallelism(1);
        DataStream<Long> mapper = source.keyBy(e -> e).flatMap(new MixedFlatMapper()).setParallelism(1);
        mapper.print().setParallelism(1);
        env.execute();
    }

    @Test
    public void testWordCountWithHDFS() throws Exception {
        Configuration config = getCommonConfiguration();
        config.set(ForStOptions.FOR_ST_MODE, ForStOptions.ForStMode.REMOTE);
        config.set(ForStOptions.FOR_ST_WORKING_DIR, "hdfs://master-1-1.c-c251b6f4b234d138.cn-beijing.emr.aliyuncs.com:9000/tmp/testWordCountWithHDFS");
        config.set(CHECKPOINTS_DIRECTORY, "hdfs://master-1-1.c-c251b6f4b234d138.cn-beijing.emr.aliyuncs.com:9000/tmp/testWordCountWithHDFS-checkpoint");
        FileSystem.initialize(config, null);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.enableCheckpointing(500);
        DataStream<String> source = WordSource.getSource(env, 100, 50, 50).setParallelism(1);
        DataStream<Long> mapper = source.keyBy(e -> e).flatMap(new MixedFlatMapper()).setParallelism(1);
        mapper.print().setParallelism(1);
        env.execute();
    }

    @Test
    public void syncExeWordCountWithASyncAPI() throws Exception {
        Configuration config = getCommonConfiguration();
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        FileSystem.initialize(config, null);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.enableCheckpointing(500);
        DataStream<String> source = WordSource.getSource(env, 1000, 10, 50).setParallelism(1);
        DataStream<Long> mapper = source.keyBy(e -> e).flatMap(new MixedFlatMapper()).setParallelism(1);
        mapper.print().setParallelism(1);
        env.execute();
    }

    public static class MixedFlatMapper extends RichFlatMapFunction<String, Long> {

        private transient AsyncValueState<Integer> asyncWordCounter;

        public MixedFlatMapper() {
        }

        @Override
        public void flatMap(String in, Collector<Long> out) throws IOException {
            asyncWordCounter.value().thenAccept(currentValue -> {
                if (currentValue != null) {
                    asyncWordCounter.update(currentValue + 1).thenAccept(empty -> {
                        out.collect(currentValue + 1L);
                    });
                } else {
                    asyncWordCounter.update(1).thenAccept(empty -> {
                        out.collect(1L);
                    });
                }
            });
        }

        @Override
        public void open(Configuration config) {
            AsyncValueStateDescriptor<Integer> descriptor =
                    new AsyncValueStateDescriptor<>(
                            "wc",
                            TypeInformation.of(new TypeHint<Integer>(){}));
            asyncWordCounter = getRuntimeContext().getAsyncState(descriptor);
        }
    }
}
