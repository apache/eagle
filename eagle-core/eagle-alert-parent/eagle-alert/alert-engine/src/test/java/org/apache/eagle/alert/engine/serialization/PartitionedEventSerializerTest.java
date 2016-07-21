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

import java.io.IOException;
import java.util.BitSet;

import org.apache.commons.lang.time.StopWatch;
import org.apache.eagle.alert.engine.mock.MockSampleMetadataFactory;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.serialization.impl.PartitionedEventSerializerImpl;
import org.apache.eagle.alert.utils.TimePeriodUtils;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.serialization.DefaultKryoFactory;
import backtype.storm.serialization.DefaultSerializationDelegate;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;


public class PartitionedEventSerializerTest {
    private final static Logger LOG = LoggerFactory.getLogger(PartitionedEventSerializerTest.class);
    @SuppressWarnings("deprecation")
    @Test
    public void testPartitionEventSerialization() throws IOException {
        PartitionedEvent partitionedEvent = MockSampleMetadataFactory.createPartitionedEventGroupedByName("sampleStream",System.currentTimeMillis());;
        PartitionedEventSerializerImpl serializer = new PartitionedEventSerializerImpl(MockSampleMetadataFactory::createSampleStreamDefinition);

        ByteArrayDataOutput dataOutput1 = ByteStreams.newDataOutput();
        serializer.serialize(partitionedEvent,dataOutput1);
        byte[] serializedBytes = dataOutput1.toByteArray();
        PartitionedEvent deserializedEvent = serializer.deserialize(ByteStreams.newDataInput(serializedBytes));
        Assert.assertEquals(partitionedEvent,deserializedEvent);

        PartitionedEventSerializerImpl compressSerializer = new PartitionedEventSerializerImpl(MockSampleMetadataFactory::createSampleStreamDefinition,true);

        byte[] serializedBytesCompressed = compressSerializer.serialize(partitionedEvent);
        PartitionedEvent deserializedEventCompressed = compressSerializer.deserialize(serializedBytesCompressed);
        Assert.assertEquals(partitionedEvent,deserializedEventCompressed);

        PartitionedEventDigestSerializer serializer2 = new PartitionedEventDigestSerializer(MockSampleMetadataFactory::createSampleStreamDefinition);
        ByteArrayDataOutput dataOutput2 = ByteStreams.newDataOutput();
        serializer2.serialize(partitionedEvent,dataOutput2);
        byte[] serializedBytes2 = dataOutput2.toByteArray();
        ByteArrayDataInput dataInput2 = ByteStreams.newDataInput(serializedBytes2);
        PartitionedEvent deserializedEvent2 = serializer2.deserialize(dataInput2);
        Assert.assertEquals(partitionedEvent,deserializedEvent2);

        byte[] javaSerialization = new DefaultSerializationDelegate().serialize(partitionedEvent);
        Kryo kryo = new DefaultKryoFactory.KryoSerializableDefault();
        Output output = new Output(10000);
        kryo.writeClassAndObject(output,partitionedEvent);
        byte[] kryoBytes = output.toBytes();
        Input input = new Input(kryoBytes);
        PartitionedEvent kryoDeserializedEvent = (PartitionedEvent) kryo.readClassAndObject(input);
        Assert.assertEquals(partitionedEvent,kryoDeserializedEvent);
        LOG.info("\nCached Stream:{}\nCompressed Cached Stream :{}\nCached Stream + Cached Partition: {}\nJava Native: {}\nKryo: {}\nKryo + Cached Stream: {}\nKryo + Cached Stream + Cached Partition: {}",serializedBytes.length,serializedBytesCompressed.length,serializedBytes2.length,javaSerialization.length,kryoBytes.length,kryoSerialize(serializedBytes).length,kryoSerialize(serializedBytes2).length);
    }
    @SuppressWarnings("deprecation")
    @Test
    public void testPartitionEventSerializationEfficiency() throws IOException {
        PartitionedEvent partitionedEvent = MockSampleMetadataFactory.createPartitionedEventGroupedByName("sampleStream",System.currentTimeMillis());;
        PartitionedEventSerializerImpl serializer = new PartitionedEventSerializerImpl(MockSampleMetadataFactory::createSampleStreamDefinition);

        int count = 100000;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        int i = 0;
        while(i<count) {
            ByteArrayDataOutput dataOutput1 = ByteStreams.newDataOutput();
            serializer.serialize(partitionedEvent, dataOutput1);
            byte[] serializedBytes = dataOutput1.toByteArray();
            PartitionedEvent deserializedEvent = serializer.deserialize(ByteStreams.newDataInput(serializedBytes));
            Assert.assertEquals(partitionedEvent, deserializedEvent);
            i++;
        }
        stopWatch.stop();
        LOG.info("Cached Stream: {} ms",stopWatch.getTime());
        stopWatch.reset();
        PartitionedEventSerializerImpl compressSerializer = new PartitionedEventSerializerImpl(MockSampleMetadataFactory::createSampleStreamDefinition,true);
        i = 0;
        stopWatch.start();
        while(i<count) {
            byte[] serializedBytesCompressed = compressSerializer.serialize(partitionedEvent);
            PartitionedEvent deserializedEventCompressed = compressSerializer.deserialize(serializedBytesCompressed);
            Assert.assertEquals(partitionedEvent, deserializedEventCompressed);
            i++;
        }
        stopWatch.stop();
        LOG.info("Compressed Cached Stream: {} ms",stopWatch.getTime());
        stopWatch.reset();

        i = 0;
        stopWatch.start();
        while(i<count) {
            PartitionedEventDigestSerializer serializer2 = new PartitionedEventDigestSerializer(MockSampleMetadataFactory::createSampleStreamDefinition);
            ByteArrayDataOutput dataOutput2 = ByteStreams.newDataOutput();
            serializer2.serialize(partitionedEvent, dataOutput2);
            byte[] serializedBytes2 = dataOutput2.toByteArray();
            ByteArrayDataInput dataInput2 = ByteStreams.newDataInput(serializedBytes2);
            PartitionedEvent deserializedEvent2 = serializer2.deserialize(dataInput2);
            Assert.assertEquals(partitionedEvent, deserializedEvent2);
            i++;
        }
        stopWatch.stop();
        LOG.info("Cached Stream&Partition: {} ms",stopWatch.getTime());
        stopWatch.reset();
        i = 0;
        stopWatch.start();
        while(i<count) {
            byte[] javaSerialization = new DefaultSerializationDelegate().serialize(partitionedEvent);
            PartitionedEvent javaSerializedEvent = (PartitionedEvent) new DefaultSerializationDelegate().deserialize(javaSerialization);
            Assert.assertEquals(partitionedEvent, javaSerializedEvent);
            i++;
        }
        stopWatch.stop();
        LOG.info("Java Native: {} ms",stopWatch.getTime());
        stopWatch.reset();
        i = 0;
        stopWatch.start();
        Kryo kryo = new DefaultKryoFactory.KryoSerializableDefault();
        while(i<count) {
            Output output = new Output(10000);
            kryo.writeClassAndObject(output, partitionedEvent);
            byte[] kryoBytes = output.toBytes();
            Input input = new Input(kryoBytes);
            PartitionedEvent kryoDeserializedEvent = (PartitionedEvent) kryo.readClassAndObject(input);
            Assert.assertEquals(partitionedEvent, kryoDeserializedEvent);
            i++;
        }
        stopWatch.stop();
        LOG.info("Kryo: {} ms",stopWatch.getTime());
    }

    /**
     * Kryo Serialization Length = Length of byte[] + 2
     */
    @Test
    public void testKryoByteArraySerialization(){
        Kryo kryo = new DefaultKryoFactory.KryoSerializableDefault();
        byte[] bytes = new byte[]{0,1,2,3,4,5,6,7,8,9};
        Output output = new Output(1000);
        kryo.writeObject(output,bytes);
        Assert.assertEquals(bytes.length + 2,output.toBytes().length);
    }

    private byte[] kryoSerialize(Object object){
        Kryo kryo = new DefaultKryoFactory.KryoSerializableDefault();
        Output output = new Output(100000);
        kryo.writeClassAndObject(output,object);
        return output.toBytes();
    }

    @Test
    public void testBitSet(){
        BitSet bitSet = new BitSet();
        bitSet.set(0,true); // 1
        bitSet.set(1,false); // 0
        bitSet.set(2,true); // 1
        LOG.info("Bit Set Size: {}",bitSet.size());
        LOG.info("Bit Set Byte[]: {}",bitSet.toByteArray());
        LOG.info("Bit Set Byte[]: {}",bitSet.toLongArray());
        LOG.info("BitSet[0]: {}",bitSet.get(0));
        LOG.info("BitSet[1]: {}",bitSet.get(1));
        LOG.info("BitSet[1]: {}",bitSet.get(2));

        byte[] bytes = bitSet.toByteArray();

        BitSet bitSet2 = BitSet.valueOf(bytes);

        LOG.info("Bit Set Size: {}",bitSet2.size());
        LOG.info("Bit Set Byte[]: {}",bitSet2.toByteArray());
        LOG.info("Bit Set Byte[]: {}",bitSet2.toLongArray());
        LOG.info("BitSet[0]: {}",bitSet2.get(0));
        LOG.info("BitSet[1]: {}",bitSet2.get(1));
        LOG.info("BitSet[1]: {}",bitSet2.get(2));


        BitSet bitSet3 = new BitSet();
        bitSet3.set(0,true);
        Assert.assertEquals(1,bitSet3.length());

        BitSet bitSet4 = new BitSet();
        bitSet4.set(0,false);
        Assert.assertEquals(0,bitSet4.length());
        Assert.assertFalse(bitSet4.get(1));
        Assert.assertFalse(bitSet4.get(2));
    }

    @Test
    public void testPeriod(){
        Assert.assertEquals(30*60*1000, TimePeriodUtils.getMillisecondsOfPeriod(Period.parse("PT30m")));
        Assert.assertEquals(30*60*1000, TimePeriodUtils.getMillisecondsOfPeriod(Period.millis(30*60*1000)));
        Assert.assertEquals("PT1800S", Period.millis(30*60*1000).toString());
    }

    @Test
    public void testPartitionType(){

    }
}