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
import java.util.Comparator;

import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.utils.SerializableUtils;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;


public class PartitionedEventGroupSerializer implements GroupSerializer<PartitionedEvent[]> {
    private static final GroupSerializer<byte[]> delegate = Serializer.BYTE_ARRAY;

    private static PartitionedEvent[] deserialize(byte[] bytes){
        return (PartitionedEvent[]) SerializableUtils.deserializeFromCompressedByteArray(bytes,"deserialize as stream event");
    }

    private static byte[] serialize(PartitionedEvent[] events){
        return SerializableUtils.serializeToCompressedByteArray(events);
    }

    @Override
    public int valueArraySearch(Object keys, PartitionedEvent[] key) {
        return delegate.valueArraySearch(keys,serialize(key));
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int valueArraySearch(Object keys, PartitionedEvent[] key, Comparator comparator) {
        return delegate.valueArraySearch(keys,serialize(key),comparator);
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {
        delegate.valueArraySerialize(out,vals);
    }

    @Override
    public Object valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        return delegate.valueArrayDeserialize(in,size);
    }

    @Override
    public PartitionedEvent[] valueArrayGet(Object vals, int pos) {
        return deserialize(delegate.valueArrayGet(vals,pos));
    }

    @Override
    public int valueArraySize(Object vals) {
        return delegate.valueArraySize(vals);
    }

    @Override
    public Object valueArrayEmpty() {
        return delegate.valueArrayEmpty();
    }

    @Override
    public Object valueArrayPut(Object vals, int pos, PartitionedEvent[] newValue) {
        return delegate.valueArrayPut(vals,pos,serialize(newValue));
    }

    @Override
    public Object valueArrayUpdateVal(Object vals, int pos, PartitionedEvent[] newValue) {
        return delegate.valueArrayUpdateVal(vals,pos,serialize(newValue));
    }

    @Override
    public Object valueArrayFromArray(Object[] objects) {
        return delegate.valueArrayFromArray(objects);
    }

    @Override
    public Object valueArrayCopyOfRange(Object vals, int from, int to) {
        return delegate.valueArrayCopyOfRange(vals,from,to);
    }

    @Override
    public Object valueArrayDeleteValue(Object vals, int pos) {
        return delegate.valueArrayDeleteValue(vals,pos);
    }

    @Override
    public void serialize(@NotNull DataOutput2 out, @NotNull PartitionedEvent[] value) throws IOException {
        delegate.serialize(out,serialize(value));
    }

    @Override
    public PartitionedEvent[] deserialize(@NotNull DataInput2 input, int available) throws IOException {
        return deserialize(delegate.deserialize(input,available));
    }
}