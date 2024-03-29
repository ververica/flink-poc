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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.table.planner.plan.utils.RankProcessStrategy;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test RankProcessStrategy json ser/de. */
class RankProcessStrategySerdeTest {

    @Test
    void testRankRange() throws JsonProcessingException {
        ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();
        RankProcessStrategy[] strategies =
                new RankProcessStrategy[] {
                    RankProcessStrategy.UNDEFINED_STRATEGY,
                    RankProcessStrategy.APPEND_FAST_STRATEGY,
                    RankProcessStrategy.RETRACT_STRATEGY,
                    new RankProcessStrategy.UpdateFastStrategy(new int[] {1, 2})
                };
        for (RankProcessStrategy strategy : strategies) {
            RankProcessStrategy result =
                    mapper.readValue(
                            mapper.writeValueAsString(strategy), RankProcessStrategy.class);
            assertThat(result.toString()).isEqualTo(strategy.toString());
        }
    }
}
