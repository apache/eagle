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
package org.apache.eagle.alert.engine.serialization.impl;

import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.engine.serialization.PartitionedEventSerializer;
import org.apache.eagle.alert.engine.serialization.SerializationMetadataProvider;
import org.apache.eagle.alert.engine.serialization.Serializer;
import org.apache.eagle.alert.engine.utils.CompressionUtils;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Stream Metadata Cached Serializer
 *
 * <p> Performance:
 *
 * 1) VS Kryo Direct: reduce 73.4% space (bytes) and 42.5 % time (ms).
 * 2) VS Java Native: reduce 92.5% space (bytes) and 94.2% time (ms)
 * </p>
 *
 * <p>Tips:
 * 1) Without-compression performs better than with compression for small event
 * </p>
 *
 * <p>TODO: Cache Partition would save little space but almost half of serialization time, how to balance the performance?</p>
 *
 * @see PartitionedEvent
 */
public class PartitionedEventSerializerImpl implements Serializer<PartitionedEvent>, PartitionedEventSerializer {
    private final StreamEventSerializer streamEventSerializer;
    private final Serializer<StreamPartition> streamPartitionSerializer;
    private final boolean compress;

    /**
     * @param serializationMetadataProvider metadata provider.
     * @param compress                      false by default.
     */
    public PartitionedEventSerializerImpl(SerializationMetadataProvider serializationMetadataProvider, boolean compress) {
        this.streamEventSerializer = new StreamEventSerializer(serializationMetadataProvider);
        this.streamPartitionSerializer = StreamPartitionSerializer.INSTANCE;
        this.compress = compress;
    }

    public PartitionedEventSerializerImpl(SerializationMetadataProvider serializationMetadataProvider) {
        this.streamEventSerializer = new StreamEventSerializer(serializationMetadataProvider);
        this.streamPartitionSerializer = StreamPartitionSerializer.INSTANCE;
        this.compress = false;
    }

    @Override
    public void serialize(PartitionedEvent entity, DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(entity.getPartitionKey());
        streamEventSerializer.serialize(entity.getEvent(), dataOutput);
        streamPartitionSerializer.serialize(entity.getPartition(), dataOutput);
    }

    @Override
    public byte[] serialize(PartitionedEvent entity) throws IOException {
        ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput();
        this.serialize(entity, dataOutput);
        return compress ? CompressionUtils.compress(dataOutput.toByteArray()) : dataOutput.toByteArray();
    }

    @Override
    public PartitionedEvent deserialize(DataInput dataInput) throws IOException {
        PartitionedEvent event = new PartitionedEvent();
        event.setPartitionKey(dataInput.readLong());
        StreamEvent streamEvent = streamEventSerializer.deserialize(dataInput);
        StreamPartition partition = streamPartitionSerializer.deserialize(dataInput);
        event.setEvent(streamEvent);
        partition.setStreamId(streamEvent.getStreamId());
        event.setPartition(partition);
        return event;
    }


    @Override
    public PartitionedEvent deserialize(byte[] bytes) throws IOException {
        return this.deserialize(ByteStreams.newDataInput(compress ? CompressionUtils.decompress(bytes) : bytes));
    }
}