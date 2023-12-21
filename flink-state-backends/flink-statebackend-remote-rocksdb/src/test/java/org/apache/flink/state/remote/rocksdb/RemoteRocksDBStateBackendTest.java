package org.apache.flink.state.remote.rocksdb;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import java.util.Collections;

import static org.apache.flink.state.remote.rocksdb.RemoteRocksDBOptions.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
    @SuppressWarnings("unchecked")
    public void testValueState() throws Exception {
        ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

        TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceSerializer.INSTANCE;

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            ValueState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            @SuppressWarnings("unchecked")
            InternalKvState<Integer, VoidNamespace, String> kvState =
                    (InternalKvState<Integer, VoidNamespace, String>) state;

            // this is only available after the backend initialized the serializer
            TypeSerializer<String> valueSerializer = kvId.getSerializer();

            // some modifications to the state
            backend.setCurrentKey(1);
            assertNull(state.value());
            assertNull(
                    getSerializedValue(
                            kvState,
                            1,
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            valueSerializer));
            state.update("1");
            backend.setCurrentKey(2);
            assertNull(state.value());
            assertNull(
                    getSerializedValue(
                            kvState,
                            2,
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            valueSerializer));
            state.update("2");
            backend.setCurrentKey(1);
            assertEquals("1", state.value());
            assertEquals(
                    "1",
                    getSerializedValue(
                            kvState,
                            1,
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            valueSerializer));

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
            assertEquals(
                    "u1",
                    getSerializedValue(
                            kvState,
                            1,
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            valueSerializer));
            backend.setCurrentKey(2);
            assertEquals("u2", state.value());
            assertEquals(
                    "u2",
                    getSerializedValue(
                            kvState,
                            2,
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            valueSerializer));
            backend.setCurrentKey(3);
            assertEquals("u3", state.value());
            assertEquals(
                    "u3",
                    getSerializedValue(
                            kvState,
                            3,
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            valueSerializer));
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
            TypeSerializer<K> keySerializer) throws Exception {
        return createKeyedBackend(keySerializer, env);
    }

    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer, Environment env) throws Exception {
        return createKeyedBackend(keySerializer, 10, new KeyGroupRange(0, 9), env);
    }

    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            Environment env)
            throws Exception {

        CheckpointableKeyedStateBackend<K> backend =
                getStateBackend()
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

    protected ConfigurableStateBackend getStateBackend() throws IOException {
        RemoteRocksDBStateBackend backend = new RemoteRocksDBStateBackend();
        Configuration configuration = new Configuration();
        configuration.set(REMOTE_ROCKSDB_MODE, RemoteRocksDBMode.REMOTE);
        configuration.set(REMOTE_ROCKSDB_WORKING_DIR, "hdfs://localhost:9000");
        backend = backend.configure(configuration, Thread.currentThread().getContextClassLoader());
        return backend;
    }
}
