package org.apache.flink.runtime.state.internal.async;

import org.apache.flink.api.common.state.async.AsyncValueState;
import org.apache.flink.runtime.state.async.internal.InternalAsyncState;
import org.apache.flink.runtime.state.internal.InternalKvState;

public interface InternalAsyncValueState<K, N, T> extends InternalAsyncState<K, N, T>, AsyncValueState<T> {
}
