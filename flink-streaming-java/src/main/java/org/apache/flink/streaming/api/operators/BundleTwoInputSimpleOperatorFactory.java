/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple factory which just wrap existed {@link StreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
@Internal
public class BundleTwoInputSimpleOperatorFactory<IN1, IN2, OUT> extends SimpleOperatorFactory<OUT> {

    protected final StreamOperator<OUT> operator;

    protected final BundleTwoInputStreamOperatorWrapper<IN1, IN2, OUT> wrapper;

    /** Create a SimpleOperatorFactory from existed StreamOperator. */
    @SuppressWarnings("unchecked")
    public static <IN1, IN2, OUT> BundleTwoInputSimpleOperatorFactory<IN1, IN2, OUT> of(
            TwoInputStreamOperator<IN1, IN2, OUT> operator) {
        if (operator == null) {
            return null;
        }
        return new BundleTwoInputSimpleOperatorFactory<>(operator);
    }

    protected BundleTwoInputSimpleOperatorFactory(TwoInputStreamOperator<IN1, IN2, OUT> operator) {
        super(operator);
        this.operator = checkNotNull(operator);
        this.wrapper = new BundleTwoInputStreamOperatorWrapper<>(operator);
    }

    public StreamOperator<OUT> getOperator() {
        return wrapper;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        if (operator instanceof AbstractStreamOperator) {
            ((AbstractStreamOperator) operator).setProcessingTimeService(processingTimeService);
        }
        if (operator instanceof SetupableStreamOperator) {
            ((SetupableStreamOperator) operator)
                    .setup(
                            parameters.getContainingTask(),
                            parameters.getStreamConfig(),
                            parameters.getOutput());
        }
        return (T) wrapper;
    }
}
