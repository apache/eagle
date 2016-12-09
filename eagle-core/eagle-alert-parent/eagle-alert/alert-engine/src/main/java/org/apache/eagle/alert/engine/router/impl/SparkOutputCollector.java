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

package org.apache.eagle.alert.engine.router.impl;

import org.apache.commons.collections.CollectionUtils;
import org.apache.eagle.alert.engine.coordinator.PublishPartition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.router.StreamOutputCollector;
import org.apache.eagle.alert.utils.StreamIdConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class SparkOutputCollector implements StreamOutputCollector, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SparkOutputCollector.class);
    private static final long serialVersionUID = -2281194763112737786L;

    private final List<Tuple2<Integer, PartitionedEvent>> pevents = new LinkedList<>();
    private final List<Tuple2<PublishPartition, AlertStreamEvent>> alertEvents = new LinkedList<>();

    @Override
    public void emit(String targetStreamId, PartitionedEvent partitionedEvent) throws Exception {
        pevents.add(new Tuple2<>(getPartitionIndex(targetStreamId), partitionedEvent));
    }

    @Override
    public void emit(List<Object> tuple) {
        if (CollectionUtils.isNotEmpty(tuple)) {
            PublishPartition publishPartition = (PublishPartition) tuple.get(0);
            AlertStreamEvent alertStreamEvent = (AlertStreamEvent) tuple.get(1);
            alertEvents.add(new Tuple2<>(publishPartition, alertStreamEvent));
        }
    }

    @Override
    public void ack(PartitionedEvent partitionedEvent) {

    }

    @Override
    public void fail(PartitionedEvent partitionedEvent) {

    }

    public List<Tuple2<Integer, PartitionedEvent>> flushPartitionedEvent() {
        LOG.info("SparkOutputCollector flushPartitionedEvent");
        return pevents;
    }

    public List<Tuple2<PublishPartition, AlertStreamEvent>> flushAlertStreamEvent() {
        LOG.info("SparkOutputCollector flushAlertStreamEvent");
        return alertEvents;
    }

    private Integer getPartitionIndex(String targetStreamId) {
        return StreamIdConversion.getPartitionNumByTargetId(targetStreamId);
    }
}
