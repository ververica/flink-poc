/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.remote.rocksdb;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Unit test for {@link BatchParallelIOExecutor}
 */
public class BatchParallelIOExecutorTest {

    @Test
    public void testBasic() throws Exception {
        List<Integer> keys = new ArrayList<>();
        Map<Integer, String> heapStateBackend = new HashMap<>();
        // generate data.
        for (int i = 0; i < 1000; i++) {
            Integer key = i * 3;
            String value = RandomStringUtils.random(20);
            keys.add(key);
            heapStateBackend.put(key, value);
        }
        Executor asyncIOExecutor = Executors.newFixedThreadPool(8);
        BatchParallelIOExecutor<Integer> parallelIOExecutor
                = new BatchParallelIOExecutor<>(keys, asyncIOExecutor);
        Iterable<String> result = parallelIOExecutor.fetchValues(heapStateBackend::get);
        int index = 0;
        for (String value : result) {
            Integer key = keys.get(index++);
            Assert.assertEquals(heapStateBackend.get(key), value);
        }
    }
}
