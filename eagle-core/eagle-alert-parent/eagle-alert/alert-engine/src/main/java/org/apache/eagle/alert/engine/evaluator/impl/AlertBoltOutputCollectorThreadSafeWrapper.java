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
package org.apache.eagle.alert.engine.evaluator.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.eagle.alert.engine.AlertStreamCollector;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;

/**
 * <h2>Thread Safe Mechanism</h2>
 * <ul>
 * <li>
 * emit() method is thread-safe enough to be called anywhere asynchronously in multi-thread
 * </li>
 * <li>
 * flush() method must be called synchronously, because Storm OutputCollector is not thread-safe
 * </li>
 * </ul>
 */
public class AlertBoltOutputCollectorThreadSafeWrapper implements AlertStreamCollector {
    private final OutputCollector delegate;
    private final LinkedBlockingQueue<AlertStreamEvent> queue;
    private final static Logger LOG = LoggerFactory.getLogger(AlertBoltOutputCollectorThreadSafeWrapper.class);
    private final AtomicLong lastFlushTime = new AtomicLong(System.currentTimeMillis());
    private final AutoAlertFlusher flusher;
    private final static int MAX_ALERT_DELAY_SECS = 10;

    public AlertBoltOutputCollectorThreadSafeWrapper(OutputCollector outputCollector) {
        this.delegate = outputCollector;
        this.queue = new LinkedBlockingQueue<>();
        this.flusher = new AutoAlertFlusher(this);
        this.flusher.setName(Thread.currentThread().getName() + "-alertFlusher");
        this.flusher.start();
    }

    private static class AutoAlertFlusher extends Thread {
        private final AlertBoltOutputCollectorThreadSafeWrapper collector;
        private boolean stopped = false;
        private final static Logger LOG = LoggerFactory.getLogger(AutoAlertFlusher.class);

        private AutoAlertFlusher(AlertBoltOutputCollectorThreadSafeWrapper collector) {
            this.collector = collector;
        }

        @Override
        public void run() {
            LOG.info("Starting");
            while (!this.stopped) {
                if (System.currentTimeMillis() - collector.lastFlushTime.get() >= MAX_ALERT_DELAY_SECS * 1000L) {
                    this.collector.flush();
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ignored) {
                }
            }
            LOG.info("Stopped");
        }

        public void shutdown() {
            LOG.info("Stopping");
            this.stopped = true;
        }
    }

    /**
     * Emit method can be called in multi-thread
     *
     * @param event
     */
    @Override
    public void emit(AlertStreamEvent event) {
        try {
            queue.put(event);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * Flush will be called in synchronous way like StormBolt.execute() as Storm OutputCollector is not thread-safe
     */
    @Override
    public void flush() {
        if (!queue.isEmpty()) {
            List<AlertStreamEvent> events = new ArrayList<>();
            queue.drainTo(events);
            events.forEach((event) -> delegate.emit(Arrays.asList(event.getStreamId(), event)));
            LOG.info("Flushed {} alerts", events.size());
        }
        lastFlushTime.set(System.currentTimeMillis());
    }

    @Override
    public void close() {
        this.flusher.shutdown();
    }
}