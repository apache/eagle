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

import java.util.Comparator;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.eagle.alert.engine.PartitionedEventCollector;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.sorter.BaseStreamWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.TreeMultiset;

public class StreamSortedWindowOnHeap extends BaseStreamWindow {
    private final static Logger LOG = LoggerFactory.getLogger(StreamSortedWindowOnHeap.class);
    private final TreeMultiset<PartitionedEvent> treeMultisetCache;

    /**
     * @param start start time
     * @param end end time
     * @param margin margin time
     */
    public StreamSortedWindowOnHeap(long start, long end, long margin, Comparator<PartitionedEvent> comparator ){
        super(start,end,margin);
        treeMultisetCache = TreeMultiset.create(comparator);
    }

    public StreamSortedWindowOnHeap(long start, long end, long margin){
        this(start,end,margin,new PartitionedEventTimeOrderingComparator());
    }

    @Override
    public boolean add(PartitionedEvent partitionedEvent) {
        synchronized (treeMultisetCache) {
            if (accept(partitionedEvent.getEvent().getTimestamp())) {
                treeMultisetCache.add(partitionedEvent);
                return true;
            } else {
                if(LOG.isDebugEnabled()) LOG.debug("{} is not acceptable, ignored", partitionedEvent);
                return false;
            }
        }
    }

    @Override
    protected void flush(PartitionedEventCollector collector) {
        synchronized (treeMultisetCache) {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            treeMultisetCache.forEach(collector::emit);
            int size = treeMultisetCache.size();
            treeMultisetCache.clear();
            stopWatch.stop();
            LOG.info("Flushed {} events in {} ms from {}", size, stopWatch.getTime(),this.toString());
        }
    }

    @Override
    public int size() {
        synchronized (treeMultisetCache) {
            return treeMultisetCache.size();
        }
    }
}