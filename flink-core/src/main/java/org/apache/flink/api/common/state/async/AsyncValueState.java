package org.apache.flink.api.common.state.async;

import org.apache.flink.api.common.state.State;

import java.io.IOException;

public interface AsyncValueState<T> extends State {

    void value(Callback<T> callback) throws IOException;

    void update(T value, Callback<Void> callback) throws IOException;

    void commit();
}
