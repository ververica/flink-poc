/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.executiongraph.failover.partitionrelease;

import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.Collections;
import java.util.List;

/**
 * Does not release intermediate result partitions during job execution. Relies on partitions being
 * released at the end of the job.
 */
public class NotReleasingPartitionGroupReleaseStrategy implements PartitionGroupReleaseStrategy {

    @Override
    public List<ConsumedPartitionGroup> vertexFinished(final ExecutionVertexID finishedVertex) {
        return Collections.emptyList();
    }

    @Override
    public void vertexUnfinished(final ExecutionVertexID executionVertexID) {}

    /** Factory for {@link NotReleasingPartitionGroupReleaseStrategy}. */
    public static class Factory implements PartitionGroupReleaseStrategy.Factory {

        @Override
        public PartitionGroupReleaseStrategy createInstance(
                final SchedulingTopology schedulingStrategy) {
            return new NotReleasingPartitionGroupReleaseStrategy();
        }
    }
}
