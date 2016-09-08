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

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.time.StopWatch;
import org.apache.eagle.alert.engine.mock.MockSampleMetadataFactory;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.utils.DateTimeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;

@Ignore("Ignore automatic heavy benchmark test")
public class StreamWindowBenchmarkTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(StreamWindowBenchmarkTest.class);

    public void sendDESCOrderedEventsToWindow(StreamWindow window, StreamWindowRepository.StorageType storageType, int num) {
        LOGGER.info("Sending {} events to {} ({})", num, window.getClass().getSimpleName(), storageType);
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        int i = 0;
        while (i < num) {
            PartitionedEvent event = MockSampleMetadataFactory.createPartitionedEventGroupedByName("sampleStream_1", (window.startTime() + i));
            window.add(event);
            i++;
        }
        stopWatch.stop();
        performanceReport.put(num + "\tInsertTime\t" + storageType, stopWatch.getTime());
        LOGGER.info("Inserted {} events in {} ms", num, stopWatch.getTime());
        stopWatch.reset();
        stopWatch.start();
        window.flush();
        stopWatch.stop();
        performanceReport.put(num + "\tReadTime\t" + storageType, stopWatch.getTime());
    }

    private ScheduledReporter metricReporter;
    private Map<String, Long> performanceReport;

    @Before
    public void setUp() {
        final MetricRegistry metrics = new MetricRegistry();
        metrics.registerAll(new MemoryUsageGaugeSet());
        metrics.registerAll(new GarbageCollectorMetricSet());
        metricReporter = ConsoleReporter.forRegistry(metrics)
            .filter((name, metric) -> name.matches("(.*heap|total).(usage|used)"))
//                .withLoggingLevel(Slf4jReporter.LoggingLevel.DEBUG)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
        metricReporter.start(60, TimeUnit.SECONDS);
        performanceReport = new TreeMap<>();
    }

    @After
    public void after() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> entry : performanceReport.entrySet()) {
            sb.append(String.format("%-40s\t%s\n", entry.getKey(), entry.getValue()));
        }
        LOGGER.info("\n===== Benchmark Result Report =====\n\n{}", sb.toString());
    }

    private final long start = DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-04 00:00:00,000");
    private final long stop = DateTimeUtil.humanDateToMillisecondsWithoutException("2016-05-05 00:00:00,000");
    private final long margin = (stop - start) / 3;

    private void benchmarkTest(StreamWindow window, StreamWindowRepository.StorageType storageType) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        LOGGER.info("\n===== Benchmark Test for {} ({}) =====", window.getClass().getSimpleName(), storageType);
        metricReporter.report();
        sendDESCOrderedEventsToWindow(window, storageType, 1000);
        metricReporter.report();
        sendDESCOrderedEventsToWindow(window, storageType, 10000);
        metricReporter.report();
        sendDESCOrderedEventsToWindow(window, storageType, 100000);
        metricReporter.report();
        sendDESCOrderedEventsToWindow(window, storageType, 1000000);
        metricReporter.report();
        stopWatch.stop();
        LOGGER.info("\n===== Finished in total {} ms =====\n", stopWatch.getTime());
    }

    @Test
    @Ignore
    public void testStreamWindowBenchmarkMain() {
        testStreamSortedWindowOnHeap();
        testStreamSortedWindowInSerializedMemory();
        testStreamSortedWindowOffHeap();
        testStreamSortedWindowFile();
    }

    @Test
    @Ignore
    public void testStreamSortedWindowOnHeap() {
        StreamWindow window = StreamWindowRepository.getSingletonInstance().createWindow(start, stop, margin, StreamWindowRepository.StorageType.ONHEAP);
        benchmarkTest(window, StreamWindowRepository.StorageType.ONHEAP);
        window.close();
    }

    @Test
    @Ignore
    public void testStreamSortedWindowInSerializedMemory() {
        StreamWindow window = StreamWindowRepository.getSingletonInstance().createWindow(start, stop, margin, StreamWindowRepository.StorageType.MEMORY);
        benchmarkTest(window, StreamWindowRepository.StorageType.MEMORY);
        window.close();
    }

    @Test
    @Ignore
    public void testStreamSortedWindowOffHeap() {
        StreamWindow window = StreamWindowRepository.getSingletonInstance().createWindow(start, stop, margin, StreamWindowRepository.StorageType.DIRECT_MEMORY);
        benchmarkTest(window, StreamWindowRepository.StorageType.DIRECT_MEMORY);
        window.close();
    }

    @Test
    @Ignore
    public void testStreamSortedWindowFile() {
        StreamWindow window = StreamWindowRepository.getSingletonInstance().createWindow(start, stop, margin, StreamWindowRepository.StorageType.FILE_RAF);
        benchmarkTest(window, StreamWindowRepository.StorageType.FILE_RAF);
        window.close();
    }
}