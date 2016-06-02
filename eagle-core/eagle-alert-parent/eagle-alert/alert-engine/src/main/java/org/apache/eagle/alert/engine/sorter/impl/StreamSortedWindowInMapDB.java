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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.eagle.alert.engine.PartitionedEventCollector;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.engine.sorter.BaseStreamWindow;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamSortedWindow based on MapDB to support off-heap or disk storage.
 *
 * Stable sorting algorithm
 *
 * <br/><br/>
 *
 * See <a href="http://www.mapdb.org">http://www.mapdb.org</a>
 */
public class StreamSortedWindowInMapDB extends BaseStreamWindow {
    private final String mapId;
    private BTreeMap<Long, PartitionedEvent[]> bTreeMap;
    private final static Logger LOG = LoggerFactory.getLogger(StreamSortedWindowInMapDB.class);
    private final AtomicInteger size;
    private  long replaceOpCount = 0;
    private final static PartitionedEventGroupSerializer STREAM_EVENT_GROUP_SERIALIZER = new PartitionedEventGroupSerializer();

    /**
     * @param start
     * @param end
     * @param margin
     * @param db
     * @param mapId physical map id, used to decide whether to reuse or not
     */
    @SuppressWarnings("unused")
    public StreamSortedWindowInMapDB(long start, long end, long margin,DB db,String mapId) {
        super(start, end, margin);
        this.mapId = mapId;
        try {
            bTreeMap = db.<Long, StreamEvent>treeMap(mapId).
                    keySerializer(Serializer.LONG).
                    valueSerializer(STREAM_EVENT_GROUP_SERIALIZER).
                    createOrOpen();
            LOG.debug("Created BTree map {}",mapId);
        } catch (Error error){
            LOG.info("Failed create BTree {}",mapId,error);
        }
        size = new AtomicInteger(0);
    }

    /**
     * Assumed: most of adding operation will do putting only and few require replacing.
     *
     * <ol>
 *     <li>
     * First of all, always try to put with created event directly
     * </li>
     * <li>
     * If not absent (key already exists), then append and replace,
     * replace operation will cause more consumption
     * </li>
     * </ol>
     * @param event coming-in event
     * @return whether success
     */
    @Override
    public synchronized boolean add(PartitionedEvent event) {
        long timestamp = event.getEvent().getTimestamp();
        if(accept(timestamp)) {
            boolean absent = bTreeMap.putIfAbsentBoolean(timestamp, new PartitionedEvent[]{event});
            if (!absent) {
                size.incrementAndGet();
                return true;
            } else {
                if(LOG.isDebugEnabled()) LOG.debug("Duplicated timestamp {}, will reduce performance as replacing",timestamp);
                PartitionedEvent[] oldValue = bTreeMap.get(timestamp);
                PartitionedEvent[] newValue = oldValue == null ? new PartitionedEvent[1]: Arrays.copyOf(oldValue,oldValue.length+1);
                newValue[newValue.length-1] = event;
                PartitionedEvent[] removedValue = bTreeMap.replace(timestamp,newValue);
                replaceOpCount ++;
                if(replaceOpCount % 1000 == 0){
                    LOG.warn("Too many events ({}) with overlap timestamp, may reduce insertion performance",replaceOpCount);
                }
                if(removedValue!=null) {
                    size.incrementAndGet();
                } else {
                    throw new IllegalStateException("Failed to replace key "+timestamp+" with "+newValue.length+" entities array to replace old "+oldValue.length+" entities array");
                }
                return true;
            }
        } else {
            return false;
        }
    }

    @Override
    protected synchronized void flush(PartitionedEventCollector collector) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        bTreeMap.valueIterator().forEachRemaining((events)->{
            for(PartitionedEvent event:events){
                collector.emit(event);
            }
        });
        bTreeMap.clear();
        replaceOpCount = 0;
        stopWatch.stop();
        LOG.info("Flushed {} events in {} ms", size, stopWatch.getTime());
        size.set(0);
    }

    @Override
    public synchronized void close(){
        super.close();
        bTreeMap.close();
        LOG.info("Closed {}",this.mapId);
    }

    @Override
    public synchronized int size() {
        return size.get();
    }
}