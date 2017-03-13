/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.proxy.stream.impl;

import org.apache.eagle.app.proxy.stream.StreamConfigUpdateListener;
import org.apache.eagle.app.proxy.stream.StreamMetadataUpdateService;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.StreamDesc;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class StreamMetadataUpdateServiceImpl implements StreamMetadataUpdateService {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamMetadataUpdateServiceImpl.class);

    private final ApplicationEntityService applicationEntityService;
    private final StreamConfigUpdateListener listener;
    private Map<String, StreamDesc> streamIdDescMap;

    private long startedTime = -1;
    private long lastUpdatedTime = -1;
    private static final long UPDATE_INTERVAL_MS = 10 * 1000;
    private volatile boolean running;
    private final Object lock;

    StreamMetadataUpdateServiceImpl(StreamConfigUpdateListener listener, ApplicationEntityService applicationEntityService) {
        this.applicationEntityService = applicationEntityService;
        this.lock = new Object();
        this.listener = listener;
        this.streamIdDescMap = new HashMap<>();
    }

    @Override
    public void run() {
        this.startedTime = System.currentTimeMillis();
        this.running = true;
        while (this.running) {
            try {
                updateStreamMetadata();
                Thread.sleep(UPDATE_INTERVAL_MS);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
        LOGGER.info("Shutting down");
    }

    private void updateStreamMetadata() {
        synchronized (lock) {
            LOGGER.debug("Loading stream metadata ...");
            int added = 0;
            int changed = 0;
            int removed = 0;
            int total = 0;
            Map<String, StreamDesc> latestStreamIdDescMap = new HashMap<>();
            for (ApplicationEntity appEntity : applicationEntityService.findAll()) {
                List<StreamDesc> streamDescList = appEntity.getStreams();
                if (streamDescList != null && streamDescList.size() > 0) {
                    for (StreamDesc streamDesc : streamDescList) {
                        total++;
                        latestStreamIdDescMap.put(streamDesc.getStreamId(), streamDesc);
                        if (streamIdDescMap.containsKey(streamDesc.getStreamId())
                            && !streamDesc.equals(streamIdDescMap.get(streamDesc.getStreamId()))) {
                            changed++;
                            this.listener.onStreamChanged(streamDesc);
                        } else if (!streamIdDescMap.containsKey(streamDesc.getStreamId())) {
                            added++;
                            this.listener.onStreamAdded(streamDesc);
                        }
                    }
                }
            }

            for (String streamId : streamIdDescMap.keySet()) {
                if (!latestStreamIdDescMap.containsKey(streamId)) {
                    removed++;
                    this.listener.onStreamRemoved(streamIdDescMap.get(streamId));
                }
            }
            if (added > 0 || changed > 0 || removed > 0) {
                LOGGER.info("Loaded stream metadata: total = {}, added = {}, changed = {}, removed = {}", total, added, changed, removed);
            } else {
                LOGGER.debug("Loaded stream metadata: total = {}, added = {}, changed = {}, removed = {}", total, added, changed, removed);
            }
            this.streamIdDescMap = latestStreamIdDescMap;
        }
    }

    @Override
    public Map<String, StreamDesc> getStreamDescSnapshot() {
        return streamIdDescMap;
    }

    public void shutdown() {
        this.running = false;
    }
}