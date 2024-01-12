package org.apache.flink.api.common.state.async;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class AsyncValueStateDescriptor<T>  extends StateDescriptor<AsyncValueState<T>, T> {

    public AsyncValueStateDescriptor(String name, TypeInformation<T> typeInfo) {
        super(name, typeInfo, null);
    }

    @Override
    public Type getType() {
        return Type.VALUE;
    }
}
