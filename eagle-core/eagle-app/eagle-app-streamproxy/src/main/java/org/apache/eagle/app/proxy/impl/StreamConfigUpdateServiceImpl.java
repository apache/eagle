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
package org.apache.eagle.app.proxy.impl;

import com.typesafe.config.Config;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.apache.eagle.app.proxy.StreamConfigUpdateListener;
import org.apache.eagle.app.proxy.StreamConfigUpdateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class StreamConfigUpdateServiceImpl implements StreamConfigUpdateService {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamConfigUpdateServiceImpl.class);

    private final IMetadataDao metadataDao;
    private Map<String, Config> streamMetadataCache;
    private final StreamConfigUpdateListener listener;

    private long startedTime = -1;
    private long lastUpdatedTime = -1;
    private static final long UPDATE_INTERVAL_MS = 5 * 60 * 1000;
    private volatile boolean running;
    private final Object lock;

    StreamConfigUpdateServiceImpl(IMetadataDao metadataDao, StreamConfigUpdateListener listener) {
        this.metadataDao = metadataDao;
        this.lock = new Object();
        this.streamMetadataCache = new ConcurrentHashMap<>();
        this.listener = listener;
    }

    @Override
    public void run() {
        this.startedTime = System.currentTimeMillis();
        while (this.running) {
            updateStreamMetadata();
            try {
                Thread.sleep(UPDATE_INTERVAL_MS);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
        LOGGER.info("Shutting down");
    }

    /**
     * TODO: Support separate Added/Modifed/Deleted stream metadata.
     */
    private void updateStreamMetadata() {
        synchronized (lock) {
            Map<String, Kafka2TupleMetadata> dataSourceMap = new HashMap<>();
            this.metadataDao.listDataSources().forEach((dataSource) -> {
                dataSourceMap.put(dataSource.getName(), dataSource);
            });
            streamMetadataCache.clear();
            this.metadataDao.listStreams().forEach((streamDefinition) -> {
                streamMetadataCache.put(streamDefinition.getStreamId(), new ImmutablePair<>(streamDefinition, dataSourceMap.get(streamDefinition.getDataSource())));
            });
            this.lastUpdatedTime = System.currentTimeMillis();
            this.listener.onStreamMetadataChanged(streamMetadataCache, new LinkedList<>());
        }
    }

    public void shutdown() {
        this.running = false;
    }
}