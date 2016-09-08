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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.time.StopWatch;
import org.apache.eagle.alert.engine.mock.MockPartitionedCollector;
import org.apache.eagle.alert.engine.mock.MockSampleMetadataFactory;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.sorter.impl.PartitionedEventTimeOrderingComparator;
import org.apache.eagle.alert.engine.sorter.impl.StreamSortWindowHandlerImpl;
import org.apache.eagle.alert.engine.sorter.impl.StreamTimeClockInLocalMemory;
import org.apache.eagle.alert.utils.DateTimeUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.collect.Ordering;

/**
 * -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+PrintGCTaskTimeStamps -XX:+PrintGCDetails -verbose:gc
 */
public class StreamSortHandlerTest {
    private final static Logger LOG = LoggerFactory.getLogger(StreamSortHandlerTest.class);

    static {
        LOG.info(ManagementFactory.getRuntimeMXBean().getName());
    }

    private ScheduledReporter metricReporter;

    @Before
    public void setUp() {
        final MetricRegistry metrics = new MetricRegistry();
        metrics.registerAll(new MemoryUsageGaugeSet());
        metrics.registerAll(new GarbageCollectorMetricSet());
        metricReporter = Slf4jReporter.forRegistry(metrics)
            .filter((name, metric) -> name.matches("(.*heap|pools.PS.*).usage"))
            .withLoggingLevel(Slf4jReporter.LoggingLevel.DEBUG)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
        metricReporter.start(60, TimeUnit.SECONDS);
    }

    /**
     * Used to debug window bucket lifecycle
     * <p>
     * Window period: PT1s, margin: 5s
     *
     * @throws InterruptedException
     */
    @Test
    public void testWithUnsortedEventsIn1MinuteWindow() throws InterruptedException {
        MockPartitionedCollector mockCollector = new MockPartitionedCollector();
        StreamTimeClockInLocalMemory timeClock = new StreamTimeClockInLocalMemory("sampleStream_1");
        Ordering<PartitionedEvent> timeOrdering = Ordering.from(PartitionedEventTimeOrderingComparator.INSTANCE);
        StreamSortWindowHandlerImpl sortHandler = new StreamSortWindowHandlerImpl();
        sortHandler.prepare("sampleStream_1", MockSampleMetadataFactory.createSampleStreamSortSpec("sampleStream_1", "PT1m", 5000), mockCollector);
        List<PartitionedEvent> unsortedList = new LinkedList<>();

        int i = 0;
        while (i < 1000) {
            PartitionedEvent event = MockSampleMetadataFactory.createRandomOutOfTimeOrderEventGroupedByName("sampleStream_1");
            sortHandler.nextEvent(event);
            unsortedList.add(event);
            if (event.getTimestamp() > timeClock.getTime()) {
                timeClock.moveForward(event.getTimestamp());
            }
            sortHandler.onTick(timeClock, System.currentTimeMillis());
            i++;
        }
        sortHandler.close();
        Assert.assertFalse(timeOrdering.isOrdered(unsortedList));
        Assert.assertTrue(timeOrdering.isOrdered(mockCollector.get()));
        Assert.assertTrue(mockCollector.get().size() > 0);
    }

    @Test
    public void testStreamSortHandlerWithUnsortedEventsIn1HourWindow() throws InterruptedException {
        testWithUnsortedEventsIn1hWindow(1000000);
    }

    @Test
    public void testSortedInPatient() throws InterruptedException {
        MockPartitionedCollector mockCollector = new MockPartitionedCollector();
        StreamTimeClockInLocalMemory timeClock = new StreamTimeClockInLocalMemory("sampleStream_1");
        Ordering<PartitionedEvent> timeOrdering = Ordering.from(PartitionedEventTimeOrderingComparator.INSTANCE);
        StreamSortWindowHandlerImpl sortHandler = new StreamSortWindowHandlerImpl();
        sortHandler.prepare("sampleStream_1", MockSampleMetadataFactory.createSampleStreamSortSpec("sampleStream_1", "PT1h", 5000), mockCollector);
        List<PartitionedEvent> sortedList = new LinkedList<>();

        int i = 0;
        while (i < 1000000) {
            PartitionedEvent event = MockSampleMetadataFactory.createRandomPartitionedEvent("sampleStream_1", System.currentTimeMillis() + i);
            sortHandler.nextEvent(event);
            sortedList.add(event);
            if (event.getTimestamp() > timeClock.getTime()) {
                timeClock.moveForward(event.getTimestamp());
            }
            sortHandler.onTick(timeClock, System.currentTimeMillis());
            i++;
        }
        sortHandler.close();
        Assert.assertTrue(timeOrdering.isOrdered(sortedList));
        Assert.assertTrue(timeOrdering.isOrdered(mockCollector.get()));
        Assert.assertEquals(1000000, mockCollector.get().size());
    }

    /**
     * -XX:+PrintGC
     *
     * @throws InterruptedException
     */
    @Test
    public void testWithUnsortedEventsInLargeWindowBenchmark() throws InterruptedException {
        metricReporter.report();
        testWithUnsortedEventsIn1hWindow(1000);
        metricReporter.report();
        testWithUnsortedEventsIn1hWindow(10000);
        metricReporter.report();
        testWithUnsortedEventsIn1hWindow(100000);
        metricReporter.report();
        testWithUnsortedEventsIn1hWindow(1000000);
        metricReporter.report();
//        testWithUnsortedEventsIn1hWindow(10000000);
//        metricReporter.report();
    }

    public void testWithUnsortedEventsIn1hWindow(int count) throws InterruptedException {
        MockPartitionedCollector mockCollector = new MockPartitionedCollector();
        StreamTimeClockInLocalMemory timeClock = new StreamTimeClockInLocalMemory("sampleStream_1");
        Ordering<PartitionedEvent> timeOrdering = Ordering.from(PartitionedEventTimeOrderingComparator.INSTANCE);
        StreamSortWindowHandlerImpl sortHandler = new StreamSortWindowHandlerImpl();
        sortHandler.prepare("sampleStream_1", MockSampleMetadataFactory.createSampleStreamSortSpec("sampleStream_1", "PT1h", 5000), mockCollector);
        List<PartitionedEvent> unsortedList = new LinkedList<>();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        int i = 0;
        while (i < count) {
            PartitionedEvent event = MockSampleMetadataFactory.createRandomOutOfTimeOrderEventGroupedByName("sampleStream_1");
            sortHandler.nextEvent(event);
            unsortedList.add(event);
            if (event.getEvent().getTimestamp() > timeClock.getTime()) {
                timeClock.moveForward(event.getEvent().getTimestamp());
            }
            sortHandler.onTick(timeClock, System.currentTimeMillis());
            i++;
        }
        stopWatch.stop();
        LOG.info("Produced {} events in {} ms", count, stopWatch.getTime());
        sortHandler.close();
        Assert.assertFalse(timeOrdering.isOrdered(unsortedList));
        Assert.assertTrue(timeOrdering.isOrdered(mockCollector.get()));
        Assert.assertTrue(mockCollector.get().size() >= 0);
    }

    /**
     * Used to debug window bucket lifecycle
     * <p>
     * Window period: PT1h, margin: 5s
     *
     * @throws InterruptedException
     */
    @Test
    public void testWithSortedEvents() throws InterruptedException {
        MockPartitionedCollector mockCollector = new MockPartitionedCollector();
        StreamTimeClockInLocalMemory timeClock = new StreamTimeClockInLocalMemory("sampleStream_1");
        Ordering<PartitionedEvent> timeOrdering = Ordering.from(PartitionedEventTimeOrderingComparator.INSTANCE);
        StreamSortWindowHandlerImpl sortHandler = new StreamSortWindowHandlerImpl();
        sortHandler.prepare("sampleStream_1", MockSampleMetadataFactory.createSampleStreamSortSpec("sampleStream_1", "PT1h", 5000), mockCollector);
        List<PartitionedEvent> sortedList = new LinkedList<>();

        int i = 0;
        while (i < 1000000) {
            PartitionedEvent event = MockSampleMetadataFactory.createRandomPartitionedEvent("sampleStream_1", System.currentTimeMillis() + i);
            sortHandler.nextEvent(event);
            sortedList.add(event);
            if (event.getTimestamp() > timeClock.getTime()) {
                timeClock.moveForward(event.getTimestamp());
            }
            sortHandler.onTick(timeClock, System.currentTimeMillis());
            i++;
        }
        sortHandler.close();
        Assert.assertTrue(timeOrdering.isOrdered(sortedList));
        Assert.assertTrue(timeOrdering.isOrdered(mockCollector.get()));
        Assert.assertEquals(1000000, mockCollector.get().size());
    }

    /**
     * Used to debug window bucket lifecycle
     * <p>
     * Window period: PT1h, margin: 5s
     *
     * @throws InterruptedException
     */
    @Test
    public void testWithSortedEventsAndExpireBySystemTime() throws InterruptedException {
        MockPartitionedCollector mockCollector = new MockPartitionedCollector();
        StreamTimeClockInLocalMemory timeClock = new StreamTimeClockInLocalMemory("sampleStream_1");
        Ordering<PartitionedEvent> timeOrdering = Ordering.from(PartitionedEventTimeOrderingComparator.INSTANCE);
        StreamSortWindowHandlerImpl sortHandler = new StreamSortWindowHandlerImpl();
        sortHandler.prepare("sampleStream_1", MockSampleMetadataFactory.createSampleStreamSortSpec("sampleStream_1", "PT10s", 1000), mockCollector);
        List<PartitionedEvent> sortedList = new LinkedList<>();

        PartitionedEvent event = MockSampleMetadataFactory.createRandomSortedEventGroupedByName("sampleStream_1");
        sortHandler.nextEvent(event);
        sortedList.add(event);
        timeClock.moveForward(event.getTimestamp());
        sortHandler.onTick(timeClock, System.currentTimeMillis());

        // Triggered to become expired by System time
        sortHandler.onTick(timeClock, System.currentTimeMillis() + 10 * 1000 + 1000L + 1);

        Assert.assertTrue(timeOrdering.isOrdered(sortedList));
        Assert.assertTrue(timeOrdering.isOrdered(mockCollector.get()));
        Assert.assertEquals(1, mockCollector.get().size());

        sortHandler.close();
    }

    //    @Test
    public void testWithTimerLock() throws InterruptedException {
        Timer timer = new Timer();
        List<Long> collected = new ArrayList<>();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                synchronized (collected) {
                    LOG.info("Ticking {}", DateTimeUtil.millisecondsToHumanDateWithMilliseconds(System.currentTimeMillis()));
                    collected.add(System.currentTimeMillis());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }, 0, 100);
    }
}