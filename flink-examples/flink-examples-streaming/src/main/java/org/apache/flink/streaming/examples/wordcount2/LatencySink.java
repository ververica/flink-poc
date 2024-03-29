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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


/**
 * Sink to statistic latency.
 */
public class LatencySink extends RichSinkFunction<Long> {

	private static final long serialVersionUID = 1L;

	/**
	 * Window size of histogram with second unit.
	 */
	private final int windowSize;

//	private transient Histogram latencyHist;

	public LatencySink(int windowSize) {
		this.windowSize = windowSize;
	}

	@Override
	public void invoke(Long value) {
//		latencyHist.update(System.currentTimeMillis() - value);
	}

	@Override
	public void open(Configuration config) {
//		this.latencyHist = getRuntimeContext()
//				.getMetricGroup()
//				.addGroup("WordCount")
//				.histogram("latency", new SimpleHistogram(windowSize));
	}
}
