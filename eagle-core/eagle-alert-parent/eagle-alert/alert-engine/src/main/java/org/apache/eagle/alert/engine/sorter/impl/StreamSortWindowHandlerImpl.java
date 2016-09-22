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
import java.io.Serializable;

import org.apache.eagle.alert.engine.PartitionedEventCollector;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.router.StreamSortHandler;
import org.apache.eagle.alert.engine.sorter.StreamTimeClock;
import org.apache.eagle.alert.engine.sorter.StreamWindow;
import org.apache.eagle.alert.engine.sorter.StreamWindowManager;
import org.apache.eagle.alert.utils.DateTimeUtil;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamSortWindowHandlerImpl implements StreamSortHandler, Serializable {
    private final static Logger LOG = LoggerFactory.getLogger(StreamSortWindowHandlerImpl.class);
    private StreamWindowManager windowManager;
    private StreamSortSpec streamSortSpecSpec;
    private PartitionedEventCollector outputCollector;
    private String streamId;

    public void prepare(String streamId, StreamSortSpec streamSortSpecSpec, PartitionedEventCollector outputCollector) {
        this.windowManager = new StreamWindowManagerImpl(
                Period.parse(streamSortSpecSpec.getWindowPeriod()),
                streamSortSpecSpec.getWindowMargin(),
                PartitionedEventTimeOrderingComparator.INSTANCE,
                outputCollector);
        this.streamSortSpecSpec = streamSortSpecSpec;
        this.streamId = streamId;
        this.outputCollector = outputCollector;
    }

    /**
     * Entry point to manage window lifecycle
     *
     * @param event StreamEvent
     */
    public void nextEvent(PartitionedEvent event) {
        final long eventTime = event.getEvent().getTimestamp();
        boolean handled = false;

        synchronized (this.windowManager){
            for (StreamWindow window : this.windowManager.getWindows()) {
                if (window.alive() && window.add(event)) {
                    handled = true;
                }
            }

            // No window found for the event but not too late being rejected
            if (!handled && !windowManager.reject(eventTime)) {
                // later then all events, create later window
                StreamWindow window = windowManager.addNewWindow(eventTime);
                if (window.add(event)) {
                    LOG.info("Created {} of {} at {}", window, this.streamId, DateTimeUtil.millisecondsToHumanDateWithMilliseconds(eventTime));
                    handled = true;
                }
            }
        }

        if(!handled){
            if(LOG.isDebugEnabled()) {
                LOG.debug("Drop expired event {}", event);
            }
            outputCollector.drop(event);
        }
    }

    @Override
    public void onTick(StreamTimeClock clock,long globalSystemTime) {
        windowManager.onTick(clock, globalSystemTime);
    }

    @Override
    public void close() {
        try {
            windowManager.close();
        } catch (IOException e) {
            LOG.error("Got exception while closing window manager",e);
        }
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public int hashCode(){
        if(streamSortSpecSpec == null){
            throw new NullPointerException("streamSortSpec is null");
        }else{
            return streamSortSpecSpec.hashCode();
        }
    }

    public void updateOutputCollector(PartitionedEventCollector outputCollector) {
        this.outputCollector = outputCollector;
        if (this.windowManager != null) {
            StreamWindowManagerImpl windowImpl = (StreamWindowManagerImpl)this.windowManager;
            windowImpl.updateOutputCollector(outputCollector);
        }
    }
}