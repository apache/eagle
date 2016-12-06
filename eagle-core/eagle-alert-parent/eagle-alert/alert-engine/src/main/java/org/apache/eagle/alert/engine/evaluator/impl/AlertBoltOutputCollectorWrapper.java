/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.evaluator.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.eagle.alert.engine.AlertStreamCollector;
import org.apache.eagle.alert.engine.StreamContext;
import org.apache.eagle.alert.engine.coordinator.PublishPartition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;

public class AlertBoltOutputCollectorWrapper implements AlertStreamCollector {

    private static final Logger LOG = LoggerFactory.getLogger(AlertBoltOutputCollectorWrapper.class);

    private final OutputCollector delegate;
    private final Object outputLock;
    private final StreamContext streamContext;

    private volatile Set<PublishPartition> publishPartitions;

    public AlertBoltOutputCollectorWrapper(OutputCollector outputCollector, Object outputLock,
                                           StreamContext streamContext) {
        this.delegate = outputCollector;
        this.outputLock = outputLock;
        this.streamContext = streamContext;

        this.publishPartitions = new HashSet<>();
    }

    @Override
    public void emit(AlertStreamEvent event) {
        Set<PublishPartition> clonedPublishPartitions = new HashSet<>(publishPartitions);
        for (PublishPartition publishPartition : clonedPublishPartitions) {
            // skip the publish partition which is not belong to this policy
            PublishPartition cloned = publishPartition.clone();
            if (!cloned.getPolicyId().equalsIgnoreCase(event.getPolicyId())) {
                continue;
            }
            for (String column : cloned.getColumns()) {
                int columnIndex = event.getSchema().getColumnIndex(column);
                if (columnIndex < 0) {
                    LOG.warn("Column {} is not found in stream {}", column, cloned.getStreamId());
                    continue;
                }
                cloned.getColumnValues().add(event.getData()[columnIndex]);
            }

            synchronized (outputLock) {
                streamContext.counter().incr("alert_count");
                delegate.emit(Arrays.asList(cloned, event));
            }
        }
    }

    @Override
    public void flush() {
        // do nothing
    }

    @Override
    public void close() {
    }

    public synchronized void onAlertBoltSpecChange(Collection<PublishPartition> addedPublishPartitions,
                                                   Collection<PublishPartition> removedPublishPartitions,
                                                   Collection<PublishPartition> modifiedPublishPartitions) {
        Set<PublishPartition> clonedPublishPartitions = new HashSet<>(publishPartitions);
        clonedPublishPartitions.addAll(addedPublishPartitions);
        clonedPublishPartitions.removeAll(removedPublishPartitions);
        clonedPublishPartitions.addAll(modifiedPublishPartitions);
        publishPartitions = clonedPublishPartitions;
    }

}