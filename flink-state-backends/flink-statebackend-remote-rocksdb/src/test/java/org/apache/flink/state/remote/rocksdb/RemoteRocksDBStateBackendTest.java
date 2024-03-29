package org.apache.flink.state.remote.rocksdb;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.batch.BatchValueState;
import org.apache.flink.api.common.state.batch.CommittedValue;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.IOUtils;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.state.remote.rocksdb.RemoteRocksDBOptions.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RemoteRocksDBStateBackendTest {

    protected MockEnvironment env;

    @Before
    public void before() throws Exception {
        env = buildMockEnv();
    }

    private MockEnvironment buildMockEnv() throws Exception {
        return MockEnvironment.builder().setTaskStateManager(getTestTaskStateManager()).build();
    }

    protected TestTaskStateManager getTestTaskStateManager() throws IOException {
        return TestTaskStateManager.builder().build();
    }

    @Test
    public void testTowBatchValueStateWithCacheLayer() throws Exception {
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE, true);
        ValueStateDescriptor<String> kvId1 = new ValueStateDescriptor<>("id-1", String.class);
        ValueStateDescriptor<String> kvId2 = new ValueStateDescriptor<>("id-2", String.class);

        try {
            ValueState<String> leftState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId1);
            ValueState<String> rightState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId2);
            List<Integer> leftKeys = Arrays.asList(1, 2, 3, 4, 5);
            List<Integer> rightKeys = Arrays.asList(5, 6, 7, 8);
            backend.setCurrentKeys(leftKeys);
            backend.setCurrentKey(2);
            leftState.update("aa");
            assertNull(rightState.value());

            backend.setCurrentKeys(rightKeys);
            backend.setCurrentKey(5);
            rightState.update("bb");
            assertNull(leftState.value());

            backend.setCurrentKeys(leftKeys);
            backend.setCurrentKey(5);
            leftState.update("cc");
            assertEquals("bb", rightState.value());

            backend.setCurrentKeys(rightKeys);
            backend.setCurrentKey(5);
            rightState.update("ff");
            assertEquals("cc", leftState.value());

            backend.setCurrentKeys(leftKeys);
            backend.setCurrentKey(2);
            assertEquals("aa", leftState.value());
            assertNull(rightState.value());

            backend.setCurrentKeys(leftKeys);
            backend.setCurrentKey(5);
            assertEquals("cc", leftState.value());
            assertEquals("ff", rightState.value());

            backend.setCurrentKeys(rightKeys);
            backend.setCurrentKey(5);
            assertEquals("ff", rightState.value());
            assertEquals("cc", leftState.value());

            backend.clearCurrentKeysCache();

            backend.setCurrentKeys(rightKeys);
            backend.setCurrentKey(7);
            rightState.update("dd");
            assertNull(leftState.value());

            backend.setCurrentKeys(leftKeys);
            backend.setCurrentKey(2);
            assertEquals("aa", leftState.value());
            assertNull(rightState.value());

            backend.setCurrentKeys(leftKeys);
            backend.setCurrentKey(5);
            assertEquals("cc", leftState.value());
            assertEquals("ff", rightState.value());

            backend.setCurrentKeys(rightKeys);
            backend.setCurrentKey(5);
            assertEquals("ff", rightState.value());
            assertEquals("cc", leftState.value());

            backend.setCurrentKeys(rightKeys);
            backend.setCurrentKey(7);
            assertEquals("dd", rightState.value());
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @Test
    public void testBatchValueStateWithCacheLayer() throws Exception {
        ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE, true);
        try {

            ValueState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            List<Integer> keys = Arrays.asList(1, 2, 3, 4, 5);
            backend.setCurrentKeys(keys);
            for (Integer key : keys) {
                backend.setCurrentKey(key);
                assertNull(state.value());
            }

            backend.setCurrentKey(2);
            state.update("aa");
            assertEquals("aa", state.value());

            backend.setCurrentKey(4);
            state.update(null);
            assertNull(state.value());

            backend.setCurrentKey(5);
            state.update("bb");
            assertEquals("bb", state.value());

            backend.clearCurrentKeysCache();

            keys = Arrays.asList(2, 4, 5, 7, 8);
            backend.setCurrentKeys(keys);

            backend.setCurrentKey(2);
            assertEquals("aa", state.value());

            backend.setCurrentKey(4);
            assertNull(state.value());

            backend.setCurrentKey(5);
            assertEquals("bb", state.value());

            backend.setCurrentKey(7);
            assertNull(state.value());

            backend.setCurrentKey(8);
            assertNull(state.value());
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }


    @Test
    public void testBatchValueStateWithoutCacheLayer() throws Exception {
        ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

        TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceSerializer.INSTANCE;

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE, false);
        try {
            BatchValueState<String> state =
                    (BatchValueState) backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            List<Integer> keys = Arrays.asList(1, 2, 3, 4, 5);
            backend.setCurrentKeys(keys);
            Iterable<String> valueItr = state.values();
            Iterator<String> iterator = valueItr.iterator();
            for (int i = 0; i < keys.size(); i++) {
                assertTrue(iterator.hasNext());
                assertNull(iterator.next());
            }

            List<CommittedValue<String>> updateValues = new ArrayList<>();
            updateValues.add(CommittedValue.of("aa", CommittedValue.CommittedValueType.UPDATE));
            updateValues.add(CommittedValue.of("bb", CommittedValue.CommittedValueType.UPDATE));
            updateValues.add(CommittedValue.of("cc", CommittedValue.CommittedValueType.UNMODIFIED));
            updateValues.add(CommittedValue.of("dd", CommittedValue.CommittedValueType.UPDATE));
            updateValues.add(CommittedValue.of("ee", CommittedValue.CommittedValueType.UPDATE));
            state.update(updateValues);

            valueItr = state.values();
            iterator = valueItr.iterator();
            assertTrue(iterator.hasNext());
            assertEquals("aa", iterator.next());
            assertEquals("bb", iterator.next());
            assertNull(iterator.next());
            assertEquals("dd", iterator.next());
            assertEquals("ee", iterator.next());

            keys = Arrays.asList(3, 4, 5, 6, 7);
            backend.setCurrentKeys(keys);

            updateValues.clear();
            updateValues.add(CommittedValue.of("ff", CommittedValue.CommittedValueType.UPDATE));
            updateValues.add(CommittedValue.of("gg", CommittedValue.CommittedValueType.DELETE));
            updateValues.add(CommittedValue.of("hh", CommittedValue.CommittedValueType.UNMODIFIED));
            updateValues.add(CommittedValue.of("ii", CommittedValue.CommittedValueType.UNMODIFIED));
            updateValues.add(CommittedValue.of("jj", CommittedValue.CommittedValueType.UPDATE));
            state.update(updateValues);

            valueItr = state.values();
            iterator = valueItr.iterator();
            assertTrue(iterator.hasNext());
            assertEquals("ff", iterator.next());
            assertNull(iterator.next());
            assertEquals("ee", iterator.next());
            assertNull(iterator.next());
            assertEquals("jj", iterator.next());

            keys = Arrays.asList(3, 5, 7, 8, 9);
            backend.setCurrentKeys(keys);

            updateValues.clear();
            updateValues.add(CommittedValue.of("kk", CommittedValue.CommittedValueType.UPDATE));
            updateValues.add(CommittedValue.ofDeletedValue());
            updateValues.add(CommittedValue.of("ll", CommittedValue.CommittedValueType.UNMODIFIED));
            updateValues.add(CommittedValue.of("mm", CommittedValue.CommittedValueType.UNMODIFIED));
            updateValues.add(CommittedValue.of("nn", CommittedValue.CommittedValueType.UPDATE));
            state.update(updateValues);

            valueItr = state.values();
            iterator = valueItr.iterator();
            assertTrue(iterator.hasNext());
            assertEquals("kk", iterator.next());
            assertNull(iterator.next());
            assertEquals("jj", iterator.next());
            assertNull(iterator.next());
            assertEquals("nn", iterator.next());
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testValueState() throws Exception {
        ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

        TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceSerializer.INSTANCE;

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE, false);
        try {
            BatchValueState<String> state =
                    (BatchValueState) backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            @SuppressWarnings("unchecked")
            InternalKvState<Integer, VoidNamespace, String> kvState =
                    (InternalKvState<Integer, VoidNamespace, String>) state;

            // this is only available after the backend initialized the serializer
            TypeSerializer<String> valueSerializer = kvId.getSerializer();

            // some modifications to the state
            backend.setCurrentKey(1);
            assertNull(state.value());
//            assertNull(
//                    getSerializedValue(
//                            kvState,
//                            1,
//                            keySerializer,
//                            VoidNamespace.INSTANCE,
//                            namespaceSerializer,
//                            valueSerializer));
            state.update("1");
            backend.setCurrentKey(2);
            assertNull(state.value());
//            assertNull(
//                    getSerializedValue(
//                            kvState,
//                            2,
//                            keySerializer,
//                            VoidNamespace.INSTANCE,
//                            namespaceSerializer,
//                            valueSerializer));
            state.update("2");
            backend.setCurrentKey(1);
            assertEquals("1", state.value());
//            assertEquals(
//                    "1",
//                    getSerializedValue(
//                            kvState,
//                            1,
//                            keySerializer,
//                            VoidNamespace.INSTANCE,
//                            namespaceSerializer,
//                            valueSerializer));

            // make some more modifications
            backend.setCurrentKey(1);
            state.update("u1");
            backend.setCurrentKey(2);
            state.update("u2");
            backend.setCurrentKey(3);
            state.update("u3");

            // validate the original state
            backend.setCurrentKey(1);
            assertEquals("u1", state.value());
//            assertEquals(
//                    "u1",
//                    getSerializedValue(
//                            kvState,
//                            1,
//                            keySerializer,
//                            VoidNamespace.INSTANCE,
//                            namespaceSerializer,
//                            valueSerializer));
            backend.setCurrentKey(2);
            assertEquals("u2", state.value());
//            assertEquals(
//                    "u2",
//                    getSerializedValue(
//                            kvState,
//                            2,
//                            keySerializer,
//                            VoidNamespace.INSTANCE,
//                            namespaceSerializer,
//                            valueSerializer));
            backend.setCurrentKey(3);
            assertEquals("u3", state.value());
//            assertEquals(
//                    "u3",
//                    getSerializedValue(
//                            kvState,
//                            3,
//                            keySerializer,
//                            VoidNamespace.INSTANCE,
//                            namespaceSerializer,
//                            valueSerializer));
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    /** Returns the value by getting the serialized value and deserializing it if it is not null. */
    protected static <V, K, N> V getSerializedValue(
            InternalKvState<K, N, V> kvState,
            K key,
            TypeSerializer<K> keySerializer,
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer)
            throws Exception {

        byte[] serializedKeyAndNamespace =
                KvStateSerializer.serializeKeyAndNamespace(
                        key, keySerializer, namespace, namespaceSerializer);

        byte[] serializedValue =
                kvState.getSerializedValue(
                        serializedKeyAndNamespace,
                        kvState.getKeySerializer(),
                        kvState.getNamespaceSerializer(),
                        kvState.getValueSerializer());

        if (serializedValue == null) {
            return null;
        } else {
            return KvStateSerializer.deserializeValue(serializedValue, valueSerializer);
        }
    }

    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer, boolean enableCacheLayer) throws Exception {
        return createKeyedBackend(keySerializer, env, enableCacheLayer);
    }

    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer, Environment env, boolean enableCacheLayer) throws Exception {
        return createKeyedBackend(keySerializer, 10, new KeyGroupRange(0, 9), env, enableCacheLayer);
    }

    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            Environment env,
            boolean enableCacheLayer)
            throws Exception {

        CheckpointableKeyedStateBackend<K> backend =
                getStateBackend(enableCacheLayer)
                        .createKeyedStateBackend(
                                env,
                                new JobID(),
                                "test_op",
                                keySerializer,
                                numberOfKeyGroups,
                                keyGroupRange,
                                env.getTaskKvStateRegistry(),
                                TtlTimeProvider.DEFAULT,
                                new UnregisteredMetricsGroup(),
                                Collections.emptyList(),
                                new CloseableRegistry());

        return backend;
    }

    protected ConfigurableStateBackend getStateBackend(boolean enableCacheLayer) throws IOException {
        RemoteRocksDBStateBackend backend = new RemoteRocksDBStateBackend();
        Configuration configuration = new Configuration();
//        configuration.set(REMOTE_ROCKSDB_MODE, RemoteRocksDBMode.REMOTE);
//        configuration.set(REMOTE_ROCKSDB_WORKING_DIR, "hdfs://localhost:9000");
        configuration.set(REMOTE_ROCKSDB_MODE, RemoteRocksDBMode.LOCAL);
        configuration.set(REMOTE_ROCKSDB_WORKING_DIR, "/tmp/");
        if (enableCacheLayer) {
            configuration.set(REMOTE_ROCKSDB_ENABLE_CACHE_LAYER, true);
        } else {
            configuration.set(REMOTE_ROCKSDB_ENABLE_CACHE_LAYER, false);
        }
        backend = backend.configure(configuration, Thread.currentThread().getContextClassLoader());
        return backend;
    }
}
