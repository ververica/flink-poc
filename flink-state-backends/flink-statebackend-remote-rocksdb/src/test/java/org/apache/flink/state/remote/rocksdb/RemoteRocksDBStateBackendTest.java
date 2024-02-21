package org.apache.flink.state.remote.rocksdb;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.async.AsyncValueState;
import org.apache.flink.api.common.state.async.AsyncValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.async.BatchingComponent;
import org.apache.flink.runtime.state.async.RecordContext;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.IOUtils;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

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
        AsyncValueStateDescriptor<String> kvId = new AsyncValueStateDescriptor<>("id", TypeInformation.of(String.class));

        BatchingComponent<Integer, Integer> batchingComponent = new BatchingComponent<>(new SyncMailboxExecutor(), 1, 1);
        CheckpointableKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE, batchingComponent);
        try {
            AsyncValueState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            // some modifications to the state
            backend.setCurrentKey(1);
            backend.setCurrentRecordContext(new RecordContext<>(1, 1, batchingComponent));
            assertValueEquals(state, null);
            state.update("1");

            backend.setCurrentKey(2);
            backend.setCurrentRecordContext(new RecordContext<>(2, 2, batchingComponent));
            assertValueEquals(state, null);
            state.update("2");
            backend.setCurrentKey(1);
            backend.setCurrentRecordContext(new RecordContext<>(1, 1, batchingComponent));
            assertValueEquals(state, "1");

            // make some more modifications
            backend.setCurrentKey(1);
            backend.setCurrentRecordContext(new RecordContext<>(1, 1, batchingComponent));
            state.update("u1");
            backend.setCurrentKey(2);
            backend.setCurrentRecordContext(new RecordContext<>(2, 2, batchingComponent));
            state.update("u2");
            backend.setCurrentKey(3);
            backend.setCurrentRecordContext(new RecordContext<>(3, 3, batchingComponent));
            state.update("u3");

            // validate the original state
            backend.setCurrentKey(1);
            backend.setCurrentRecordContext(new RecordContext<>(1, 1, batchingComponent));
            assertValueEquals(state, "u1");
            backend.setCurrentKey(2);
            backend.setCurrentRecordContext(new RecordContext<>(2, 2, batchingComponent));
            assertValueEquals(state, "u2");
            backend.setCurrentKey(3);
            backend.setCurrentRecordContext(new RecordContext<>(3, 3, batchingComponent));
            assertValueEquals(state, "u3");
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    private void assertValueEquals(AsyncValueState<String> state, String expectValue) throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        state.value().then(v ->  {
            try {
                assertEquals(expectValue, v);
                countDownLatch.countDown();
            } catch (Throwable e) {
                error.set(e);
            }
        });
        countDownLatch.await();
        assertNull(error.get());
    }

    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer, BatchingComponent<?, K> batchingComponent) throws Exception {
        return createKeyedBackend(keySerializer, 10, new KeyGroupRange(0, 9), env, batchingComponent);
    }

    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            Environment env,
            BatchingComponent<?, K> batchingComponent)
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
                                new CloseableRegistry(),
                                1,
                                batchingComponent);

        return backend;
    }

    protected ConfigurableStateBackend getStateBackend() throws IOException {
        RemoteRocksDBStateBackend backend = new RemoteRocksDBStateBackend();
        Configuration configuration = new Configuration();
        configuration.set(REMOTE_ROCKSDB_MODE, RemoteRocksDBMode.LOCAL);
        configuration.set(REMOTE_ROCKSDB_WORKING_DIR, "/tmp/");
        configuration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        backend = backend.configure(configuration, Thread.currentThread().getContextClassLoader());
        return backend;
    }
}
