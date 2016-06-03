/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.sorter.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.engine.utils.SerializableUtils;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializerObjectArray;

/**
 * @deprecated performance is worse, should investigate
 */
public class CachedEventGroupSerializer extends GroupSerializerObjectArray<PartitionedEvent[]> {
    private Map<Integer,StreamPartition> hashCodePartitionDict = new HashMap<>();
    private void writePartitionedEvent(DataOutput2 out,PartitionedEvent event) throws IOException {
        out.packLong(event.getPartitionKey());
        int partitionHashCode = 0;
        if(event.getPartition()!=null) {
            partitionHashCode = event.getPartition().hashCode();
            if (!hashCodePartitionDict.containsKey(partitionHashCode)) {
                hashCodePartitionDict.put(partitionHashCode, event.getPartition());
            }
        }
        out.packInt(partitionHashCode);
        if(event.getEvent()!=null) {
            byte[] eventBytes = SerializableUtils.serializeToCompressedByteArray(event.getEvent());
            out.packInt(eventBytes.length);
            out.write(eventBytes);
        } else {
            out.packInt(0);
        }
    }

    private PartitionedEvent readPartitionedEvent(DataInput2 in) throws IOException {
        PartitionedEvent event = new PartitionedEvent();
        event.setPartitionKey(in.unpackLong());
        int partitionHashCode = in.unpackInt();
        if(partitionHashCode!=0 && hashCodePartitionDict.containsKey(partitionHashCode)) {
            event.setPartition(hashCodePartitionDict.get(partitionHashCode));
        }
        int eventBytesLen = in.unpackInt();
        if(eventBytesLen > 0) {
            byte[] eventBytes = new byte[eventBytesLen];
            in.readFully(eventBytes);
            event.setEvent((StreamEvent) SerializableUtils.deserializeFromCompressedByteArray(eventBytes, "Deserialize event from bytes"));
        }
        return event;
    }

    @Override
    public void serialize(DataOutput2 out, PartitionedEvent[] value) throws IOException {
        out.packInt(value.length);
        for (PartitionedEvent event : value) {
            writePartitionedEvent(out,event);
        }
    }

    @Override
    public PartitionedEvent[] deserialize(DataInput2 in, int available) throws IOException {
        final int size = in.unpackInt();
        PartitionedEvent[] ret = new PartitionedEvent[size];
        for (int i = 0; i < size; i++) {
            ret[i] = readPartitionedEvent(in);
        }
        return ret;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public boolean equals(PartitionedEvent[] a1, PartitionedEvent[] a2) {
        return a1[0].getTimestamp() == a2[0].getTimestamp();
    }

    @Override
    public int hashCode(@NotNull PartitionedEvent[] events, int seed) {
        return new HashCodeBuilder().append(events).toHashCode();
    }

    @Override
    public int compare(PartitionedEvent[] o1, PartitionedEvent[] o2) {
        if(o1.length>0 && o2.length>0) {
            return (int) (o1[0].getTimestamp() - o2[0].getTimestamp());
        }else{
            return 0;
        }
    }
}