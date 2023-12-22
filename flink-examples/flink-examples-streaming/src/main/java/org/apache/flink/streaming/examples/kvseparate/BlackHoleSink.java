package org.apache.flink.streaming.examples.kvseparate;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class BlackHoleSink extends RichSinkFunction<String> {

    private static final long serialVersionUID = 1L;
}
