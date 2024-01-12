package org.apache.flink.api.common.state.async;

public interface Callback<T> {

    void accept(T t) throws Exception;
}
