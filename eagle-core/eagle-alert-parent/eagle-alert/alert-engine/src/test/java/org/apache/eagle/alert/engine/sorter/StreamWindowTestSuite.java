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
package org.apache.eagle.alert.engine.sorter;

import com.google.common.collect.Ordering;
import org.apache.eagle.alert.engine.mock.MockPartitionedCollector;
import org.apache.eagle.alert.engine.mock.MockSampleMetadataFactory;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.sorter.impl.PartitionedEventTimeOrderingComparator;
import org.apache.eagle.alert.engine.sorter.impl.StreamTimeClockInLocalMemory;
import org.apache.eagle.common.DateTimeUtil;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@SuppressWarnings("unused")
public class StreamWindowTestSuite {
    private final static Logger LOGGER = LoggerFactory.getLogger(StreamWindowTestSuite.class);

    private final long start = DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:00,000");
    private final long stop = DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:01:00,000");
    private final long margin = (stop - start) / 3;

    @Test
    public void testStreamSortedWindowOnHeap() {
        StreamWindow window = StreamWindowRepository.getSingletonInstance().createWindow(start, stop, margin, StreamWindowRepository.StorageType.ONHEAP);
        streamSortedWindowMustTest(window);
    }

    @Test
    public void testStreamSortedWindowInSerializedMemory() {
        StreamWindow window = StreamWindowRepository.getSingletonInstance().createWindow(start, stop, margin, StreamWindowRepository.StorageType.MEMORY);
        streamSortedWindowMustTest(window);
    }

    @Test
    public void testStreamSortedWindowOffHeap() {
        StreamWindow window = StreamWindowRepository.getSingletonInstance().createWindow(start, stop, margin, StreamWindowRepository.StorageType.DIRECT_MEMORY);
        streamSortedWindowMustTest(window);
    }

    @Test
    public void testStreamSortedWindowFile() {
        StreamWindow window = StreamWindowRepository.getSingletonInstance().createWindow(start, stop, margin, StreamWindowRepository.StorageType.FILE_RAF);
        streamSortedWindowMustTest(window);
    }

    @SuppressWarnings( {"unchecked", "rawtypes"})
    private void streamSortedWindowMustTest(StreamWindow window) {
        MockPartitionedCollector collector = new MockPartitionedCollector();
        window.register(collector);

        StreamTimeClock clock = new StreamTimeClockInLocalMemory("sampleStream_1");
        clock.moveForward(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:30,000"));

        // Current time is: "2016-05-04 00:00:30"
        window.onTick(clock, System.currentTimeMillis());

        Assert.assertTrue(window.alive());
        Assert.assertFalse(window.expired());

        // Accepted
        Assert.assertTrue(window.accept(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:00,000")));
        Assert.assertTrue(window.accept(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:01,000")));
        Assert.assertTrue(window.accept(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:30,000")));
        Assert.assertTrue(window.accept(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:40,000")));
        Assert.assertTrue(window.accept(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:50,000")));


        // Rejected
        Assert.assertFalse(window.accept(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-03 23:59:59,000")));
        Assert.assertFalse(window.accept(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:01:00,000")));
        Assert.assertFalse(window.accept(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:01:01,000")));

        // Accepted
        Assert.assertTrue(window.add(MockSampleMetadataFactory.createRandomPartitionedEvent("sampleStream_1", DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:00,000"))));

        Assert.assertTrue(window.add(MockSampleMetadataFactory.createRandomPartitionedEvent("sampleStream_1", DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:01,000"))));
        Assert.assertTrue(window.add(MockSampleMetadataFactory.createRandomPartitionedEvent("sampleStream_1", DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:50,000"))));
        Assert.assertTrue(window.add(MockSampleMetadataFactory.createRandomPartitionedEvent("sampleStream_1", DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:40,000"))));
        Assert.assertTrue(window.add(MockSampleMetadataFactory.createRandomPartitionedEvent("sampleStream_1", DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:30,000"))));

        // Should accept Duplicated
        Assert.assertTrue("Should support duplicated timestamp", window.add(MockSampleMetadataFactory.createRandomPartitionedEvent("sampleStream_1", DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:00,000"))));

        Assert.assertEquals(6, window.size());

        // Rejected
        Assert.assertFalse(window.add(MockSampleMetadataFactory.createRandomPartitionedEvent("sampleStream_1", DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-03 23:59:59,000"))));
        Assert.assertFalse(window.add(MockSampleMetadataFactory.createRandomPartitionedEvent("sampleStream_1", DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:01:00,000"))));
        Assert.assertFalse(window.add(MockSampleMetadataFactory.createRandomPartitionedEvent("sampleStream_1", DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:01:01,000"))));

        Assert.assertEquals(6, window.size());

        // Now is: "2016-05-04 00:00:55"
        clock.moveForward(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:55,000"));
        window.onTick(clock, System.currentTimeMillis());
        Assert.assertTrue(window.alive());
        Assert.assertFalse(window.expired());
        Assert.assertTrue(window.add(MockSampleMetadataFactory.createRandomPartitionedEvent("sampleStream_1", DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:35,000"))));
        Assert.assertEquals(7, window.size());

        // Flush when stream time delay too much after system time but window will still be alive
        window.onTick(clock, System.currentTimeMillis() + 1 + stop - start + margin);
        Assert.assertTrue(window.alive());
        Assert.assertFalse(window.expired());
        Assert.assertEquals(0, window.size());
        Assert.assertEquals(7, collector.size());

        Assert.assertFalse("Because window has flushed but not expired, window should reject future events < last flush stream time",
            window.add(MockSampleMetadataFactory.createRandomPartitionedEvent("sampleStream_1", DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:54,000"))));
        Assert.assertTrue("Because window has flushed but not expired, window should still accept future events >= last flush stream time",
            window.add(MockSampleMetadataFactory.createRandomPartitionedEvent("sampleStream_1", DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:56,000"))));
        Assert.assertEquals(1, window.size());
        Assert.assertEquals(7, collector.size());

        // Now is: "2016-05-04 00:01:10", not expire,
        clock.moveForward(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:01:10,000"));
        window.onTick(clock, System.currentTimeMillis() + 2 * (1 + stop - start + margin));
        Assert.assertEquals(8, collector.size());

        // Now is: "2016-05-04 00:01:20", expire
        clock.moveForward(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:01:20,000"));
        window.onTick(clock, System.currentTimeMillis());
        Assert.assertFalse(window.alive());
        Assert.assertTrue(window.expired());
        Assert.assertFalse(window.add(MockSampleMetadataFactory.createRandomPartitionedEvent("sampleStream_1", DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:35,000"))));
        Assert.assertEquals(0, window.size());

        Assert.assertEquals(8, collector.size());

        Ordering ordering = Ordering.from(PartitionedEventTimeOrderingComparator.INSTANCE);
        Assert.assertTrue(ordering.isOrdered(collector.get()));

        List<PartitionedEvent> list = collector.get();
        Assert.assertEquals(8, list.size());
        Assert.assertEquals(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:00,000"), list.get(0).getTimestamp());
        Assert.assertEquals(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:00,000"), list.get(1).getTimestamp());
        Assert.assertEquals(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:01,000"), list.get(2).getTimestamp());
        Assert.assertEquals(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:30,000"), list.get(3).getTimestamp());
        Assert.assertEquals(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:35,000"), list.get(4).getTimestamp());
        Assert.assertEquals(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:40,000"), list.get(5).getTimestamp());
        Assert.assertEquals(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:50,000"), list.get(6).getTimestamp());
        Assert.assertEquals(DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:56,000"), list.get(7).getTimestamp());
    }
}