/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package org.apache.flink.streaming.examples.wordcount2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.async.AsyncValueState;
import org.apache.flink.api.common.state.async.AsyncValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.state.forst.ForStOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.state.forst.ForStOptions.FOR_ST_MODE;
import static org.apache.flink.state.forst.ForStOptions.FOR_ST_WORKING_DIR;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.FLAT_MAP_PARALLELISM;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.JOB_NAME;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.SHARING_GROUP;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.STATE_MODE;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.TTL;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.WORD_LENGTH;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.WORD_NUMBER;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.WORD_RATE;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.configureCheckpoint;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.getConfiguration;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.setStateBackend;

/**
 * Benchmark mainly used for {@link ValueState} and only support 1 parallelism.
 */
public class WordCount {

	private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        Configuration configuration = getConfiguration(params);
        //configRemoteStateBackend(configuration);
		env.getConfig().setGlobalJobParameters(configuration);
		env.disableOperatorChaining();

		String jobName = configuration.getString(JOB_NAME);

		configureCheckpoint(env, configuration);

		String group1 = "default1";
		String group2 = "default2";
		String group3 = "default3";
		if (configuration.getBoolean(SHARING_GROUP)) {
			group1 = group2 = group3 = "default";
		}

		setStateBackend(env, configuration);

		// configure source
		int wordNumber = configuration.getInteger(WORD_NUMBER);
		int wordLength = configuration.getInteger(WORD_LENGTH);
		int wordRate = configuration.getInteger(WORD_RATE);

		DataStream<Tuple2<String, Long>> source =
				WordSource.getSource(env, wordRate, wordNumber, wordLength)
                        .slotSharingGroup(group1);

		// configure ttl
		long ttl = configuration.get(TTL).toMillis();

		FlatMapFunction<Tuple2<String, Long>, Long> flatMapFunction =
			getFlatMapFunction(configuration, ttl);
		DataStream<Long> mapper = source.keyBy(0)
				.flatMap(flatMapFunction)
				.setParallelism(configuration.getInteger(FLAT_MAP_PARALLELISM))
                .slotSharingGroup(group2);

		//mapper.print().setParallelism(1);
        mapper.addSink(new BlackholeSink<>()).slotSharingGroup(group3);

		if (jobName == null) {
			env.execute();
		} else {
			env.execute(jobName);
		}
	}

    private static void configRemoteStateBackend(Configuration config) {
        config.set(StateBackendOptions.STATE_BACKEND,
                "org.apache.flink.state.remote.rocksdb.RemoteRocksDBStateBackendFactory");
//        config.set(REMOTE_ROCKSDB_MODE, RemoteRocksDBOptions.RemoteRocksDBMode.REMOTE);
//        config.set(REMOTE_ROCKSDB_WORKING_DIR, "hdfs://localhost:9000");
        config.set(FOR_ST_MODE, ForStOptions.ForStMode.LOCAL);
        config.set(FOR_ST_WORKING_DIR, "/tmp");
    }

	private static FlatMapFunction<Tuple2<String, Long>, Long> getFlatMapFunction(Configuration configuration, long ttl) {
		JobConfig.StateMode stateMode =
			JobConfig.StateMode.valueOf(configuration.getString(STATE_MODE).toUpperCase());

		switch (stateMode) {
			case WRITE:
				return new WriteFlatMapper(ttl);
			case READ:
				return new ReadFlatMapper(configuration.getInteger(WORD_NUMBER), ttl);
			case MIXED:
			default:
				return new MixedFlatMapper(ttl);
		}
	}

	public static class CustomTypeSerializer extends TypeSerializerSingleton<Object> {

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public Object createInstance() {
			return 0;
		}

		@Override
		public Object copy(Object o) {
			return ((Integer) o).intValue();
		}

		@Override
		public Object copy(Object o, Object t1) {
			return null;
		}

		@Override
		public int getLength() {
			return 4;
		}

		@Override
		public void serialize(Object o, DataOutputView dataOutputView) throws IOException {
			System.out.println("Serializing " + o.toString());
			dataOutputView.writeInt((Integer) o);
		}

		@Override
		public Object deserialize(DataInputView dataInputView) throws IOException {
			int a = dataInputView.readInt();
			System.out.println("Deserializing " + a);
			return a;
		}

		@Override
		public Object deserialize(Object o, DataInputView dataInputView) throws IOException {
			int a = dataInputView.readInt();
			System.out.println("Deserializing " + a);
			return a;
		}

		@Override
		public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {
			dataOutputView.write(dataInputView, 4);
		}

		@Override
		public TypeSerializerSnapshot<Object> snapshotConfiguration() {
			return new CustomTypeSerializerSnapshot();
		}

	};


	static CustomTypeSerializer INSTANCE = new CustomTypeSerializer();

	public static final class CustomTypeSerializerSnapshot extends SimpleTypeSerializerSnapshot<Object> {
		public CustomTypeSerializerSnapshot() {
			super(() -> {
				return INSTANCE;
			});
		}
	}

	/**
	 * Write and read mixed mapper.
	 */
	public static class MixedFlatMapper extends RichFlatMapFunction<Tuple2<String, Long>, Long> {

		private transient AsyncValueState<Integer> wordCounter;

		private final long ttl;

		public MixedFlatMapper(long ttl) {
			this.ttl = ttl;
		}

		@Override
		public void flatMap(Tuple2<String, Long> in, Collector<Long> out) throws IOException {
            wordCounter.value().then(currentValue -> {
                if (currentValue != null) {
                    wordCounter.update(currentValue + 1).then(empty -> {
                        out.collect(currentValue + 1L);
                    });
                } else {
                    wordCounter.update(1).then(empty -> {
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
			if (ttl > 0) {
				LOG.info("Setting ttl to {}ms.", ttl);
				StateTtlConfig ttlConfig = StateTtlConfig
						.newBuilder(Time.milliseconds(ttl))
						.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
						.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
						.build();
				descriptor.enableTimeToLive(ttlConfig);
			}
			wordCounter = getRuntimeContext().getAsyncState(descriptor);
		}
	}

	/**
	 * Write mapper.
	 */
	public static class WriteFlatMapper extends RichFlatMapFunction<Tuple2<String, Long>, Long> {

		private transient ValueState<Object> wordCounter;

		private final long ttl;

		public WriteFlatMapper(long ttl) {
			this.ttl = ttl;
		}

		@Override
		public void flatMap(Tuple2<String, Long> in, Collector<Long> out) throws IOException {
			wordCounter.update(1);
			out.collect(in.f1);
		}

		@Override
		public void open(Configuration config) {
			ValueStateDescriptor<Object> descriptor =
					new ValueStateDescriptor<>(
							"wc",
							INSTANCE);
			if (ttl > 0) {
				LOG.info("Setting ttl to {}ms.", ttl);
				StateTtlConfig ttlConfig = StateTtlConfig
						.newBuilder(Time.milliseconds(ttl))
						.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
						.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
						.build();
				descriptor.enableTimeToLive(ttlConfig);
			}
			wordCounter = getRuntimeContext().getState(descriptor);
		}
	}

	/**
	 * Read mapper.
	 */
	public static class ReadFlatMapper extends RichFlatMapFunction<Tuple2<String, Long>, Long> {

		private transient ValueState<Object> wordCounter;

		private final int total;

		private final long ttl;

		private transient int count;

		public ReadFlatMapper(int total, long ttl) {
			this.total = total;
			this.ttl = ttl;
		}

		@Override
		public void flatMap(Tuple2<String, Long> in, Collector<Long> out) throws IOException {
			if (count < total) {
				wordCounter.update(1);
				count++;
				if (count == total) {
					LOG.info("switch from write to read after {} keys", count);
				}
			} else {
				wordCounter.value();
			}
			out.collect(in.f1);
		}

		@Override
		public void open(Configuration config) {
			ValueStateDescriptor<Object> descriptor =
					new ValueStateDescriptor<>(
							"wc",
							INSTANCE);
			if (ttl > 0) {
				LOG.info("Setting ttl to {}ms.", ttl);
				StateTtlConfig ttlConfig = StateTtlConfig
						.newBuilder(Time.milliseconds(ttl))
						.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
						.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
						.build();
				descriptor.enableTimeToLive(ttlConfig);
			}
			wordCounter = getRuntimeContext().getState(descriptor);
			count = 0;
		}
	}
}
