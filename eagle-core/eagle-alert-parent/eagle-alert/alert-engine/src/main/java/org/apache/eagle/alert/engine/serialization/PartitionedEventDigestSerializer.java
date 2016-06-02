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
package org.apache.eagle.alert.engine.serialization;

import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.engine.serialization.impl.StreamEventSerializer;
import org.apache.eagle.alert.engine.serialization.impl.StreamPartitionDigestSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * TODO: Seams the complexity dosen't bring enough performance improve
 *
 * @see PartitionedEvent
 */
@Deprecated
public class PartitionedEventDigestSerializer implements Serializer<PartitionedEvent> {
    private final Serializer<StreamEvent> streamEventSerializer;
    private final Serializer<StreamPartition> streamPartitionSerializer;

    public PartitionedEventDigestSerializer(SerializationMetadataProvider serializationMetadataProvider){
        this.streamEventSerializer = new StreamEventSerializer(serializationMetadataProvider);
        this.streamPartitionSerializer = StreamPartitionDigestSerializer.INSTANCE;
    }

    @Override
    public void serialize(PartitionedEvent entity, DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(entity.getPartitionKey());
        streamEventSerializer.serialize(entity.getEvent(),dataOutput);
        streamPartitionSerializer.serialize(entity.getPartition(),dataOutput);
    }

    @Override
    public PartitionedEvent deserialize(DataInput dataInput) throws IOException {
        PartitionedEvent event = new PartitionedEvent();
        event.setPartitionKey(dataInput.readLong());
        StreamEvent streamEvent = streamEventSerializer.deserialize(dataInput);
        event.setEvent(streamEvent);
        StreamPartition partition = streamPartitionSerializer.deserialize(dataInput);
        partition.setStreamId(streamEvent.getStreamId());
        event.setPartition(partition);
        return event;
    }
}