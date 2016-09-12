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
package org.apache.eagle.alert.coordination.model.internal;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A monitored stream is the unique data set in the system.
 *
 * <p>It's a combination of stream and the specific grp-by on it.
 *
 * <p>For correlation stream, it means multiple stream for a given monitored stream.
 *
 * @since Apr 27, 2016
 */
public class MonitoredStream {

    private String version;

    // the stream group that this monitored stream stands for
    private StreamGroup streamGroup = new StreamGroup();
    private List<StreamWorkSlotQueue> queues = new ArrayList<StreamWorkSlotQueue>();

    public MonitoredStream() {
    }

    public MonitoredStream(StreamGroup par) {
        this.streamGroup = par;
    }

    public StreamGroup getStreamGroup() {
        return streamGroup;
    }

    public List<StreamWorkSlotQueue> getQueues() {
        return queues;
    }

    public synchronized void addQueues(StreamWorkSlotQueue queue) {
        queues.add(queue);
    }

    public synchronized boolean removeQueue(StreamWorkSlotQueue queue) {
        return this.queues.remove(queue);
    }

    public int hashCode() {
        return new HashCodeBuilder().append(streamGroup).build();
    }

    public boolean equals(Object other) {
        if (!(other instanceof MonitoredStream)) {
            return false;
        }
        MonitoredStream o = (MonitoredStream) other;
        return Objects.equals(streamGroup, o.streamGroup);
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setQueues(List<StreamWorkSlotQueue> queues) {
        this.queues = queues;
    }

}
