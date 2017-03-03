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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.apache.eagle.app.proxy.StreamConfigUpdateListener;
import org.apache.eagle.app.proxy.StreamConfigUpdateService;
import org.apache.eagle.app.proxy.StreamProxy;
import org.apache.eagle.app.proxy.StreamProxyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Singleton
public class StreamProxyManagerImpl implements StreamProxyManager, StreamConfigUpdateListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamProxyManagerImpl.class);
    private final StreamConfigUpdateService streamConfigUpdateService;
    private final ConcurrentMap<String, StreamProxy> streamProxyConcurrentMap;

    @Inject
    public StreamProxyManagerImpl(IMetadataDao metadataDao) {
        this.streamConfigUpdateService = new StreamConfigUpdateServiceImpl(metadataDao, this);
        this.streamProxyConcurrentMap = new ConcurrentHashMap<>();
        Thread streamMetadataUpdateServiceThread = new Thread(this.streamConfigUpdateService);
        streamMetadataUpdateServiceThread.setName(StreamConfigUpdateService.class.getName());
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                streamConfigUpdateService.shutdown();
            }
        });
        streamMetadataUpdateServiceThread.start();
    }

    @Override
    public StreamProxy getStreamProxy(String streamId) {
        Preconditions.checkArgument(streamProxyConcurrentMap.containsKey(streamId));
        return streamProxyConcurrentMap.get(streamId);
    }

    @Override
    public void onStreamMetadataChanged(Map<String, Pair<StreamDefinition, Kafka2TupleMetadata>> updatedStreamMetadata, List<String> deletedStreamIds) {
        if (deletedStreamIds.size() > 0) {
            deletedStreamIds.forEach(((streamId) -> {
                LOGGER.info("Removing StreamProxy for streamId: {}", streamId);
                StreamProxy proxy = streamProxyConcurrentMap.get(streamId);
                if (proxy != null) {
                    try {
                        proxy.close();
                    } catch (IOException e) {
                        LOGGER.error("Error to close StreamProxy: {}", e.getMessage(), e);
                    } finally {
                        streamProxyConcurrentMap.remove(streamId);
                    }
                } else {
                    LOGGER.warn("StreamProxy for streamId: {} not exist", streamId);
                }
            }));
        }

        for (Map.Entry<String, Pair<StreamDefinition, Kafka2TupleMetadata>> entry : updatedStreamMetadata.entrySet()) {
            LOGGER.info("Replacing StreamProxy for streamId: {}", entry.getKey());
            if (streamProxyConcurrentMap.containsKey(entry.getKey())) {
                try {
                    streamProxyConcurrentMap.get(entry.getKey()).close();
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            StreamProxy proxy = createStreamProxy(entry.getKey(), entry.getValue().getLeft(), entry.getValue().getRight());
            streamProxyConcurrentMap.put(entry.getKey(), proxy);
        }
    }

    private StreamProxy createStreamProxy(String streamId, Config config) {
        return new StreamProxyImpl(streamId, config);
    }
}