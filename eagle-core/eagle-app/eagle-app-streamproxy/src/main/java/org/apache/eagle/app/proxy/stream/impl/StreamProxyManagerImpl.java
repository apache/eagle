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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.eagle.app.proxy.stream.StreamConfigUpdateListener;
import org.apache.eagle.app.proxy.stream.StreamMetadataUpdateService;
import org.apache.eagle.app.proxy.stream.StreamProxy;
import org.apache.eagle.app.proxy.stream.StreamProxyManager;
import org.apache.eagle.app.proxy.exception.StreamProxyException;
import org.apache.eagle.metadata.model.StreamDesc;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Singleton
public class StreamProxyManagerImpl implements StreamProxyManager, StreamConfigUpdateListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamProxyManagerImpl.class);
    private final StreamMetadataUpdateService streamMetadataUpdateService;
    private final ConcurrentMap<String, StreamProxy> streamProxyConcurrentMap;

    @Inject
    public StreamProxyManagerImpl(ApplicationEntityService applicationEntityService) {
        LOGGER.info("Initializing StreamProxyManager {}", this);
        this.streamMetadataUpdateService = new StreamMetadataUpdateServiceImpl(this, applicationEntityService);
        this.streamProxyConcurrentMap = new ConcurrentHashMap<>();
        Thread streamMetadataUpdateServiceThread = new Thread(this.streamMetadataUpdateService);
        streamMetadataUpdateServiceThread.setDaemon(true);
        streamMetadataUpdateServiceThread.setName(StreamMetadataUpdateService.class.getName());
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                streamMetadataUpdateService.shutdown();
                for (StreamProxy proxy : streamProxyConcurrentMap.values()) {
                    try {
                        proxy.close();
                    } catch (IOException e) {
                        LOGGER.error("Error to close {}: {}", proxy, e.getMessage(), e);
                    }
                }
            }
        });
        streamMetadataUpdateServiceThread.start();
    }

    @Override
    public StreamProxy getStreamProxy(String streamId) throws StreamProxyException {
        if (!streamMetadataUpdateService.getStreamDescSnapshot().containsKey(streamId)) {
            throw new StreamProxyException("Stream ID: " + streamId + " not found");
        }
        if (!streamProxyConcurrentMap.containsKey(streamId)) {
            throw new StreamProxyException("Not stream proxy instance initialized for " + streamId);
        }
        return streamProxyConcurrentMap.get(streamId);
    }

    @Override
    public Collection<StreamDesc> getAllStreamDesc() {
        return streamMetadataUpdateService.getStreamDescSnapshot().values();
    }

    @Override
    public void onStreamAdded(StreamDesc streamDesc) {
        if (streamProxyConcurrentMap.containsKey(streamDesc.getStreamId())) {
            LOGGER.warn("Adding already existing stream proxy {}", streamDesc.getStreamId());
            this.onStreamChanged(streamDesc);
            return;
        }
        LOGGER.info("Adding stream proxy {}", streamDesc.getStreamId());
        StreamProxy proxy = new StreamProxyImpl();
        proxy.open(streamDesc);
        streamProxyConcurrentMap.put(streamDesc.getStreamId(), proxy);
    }

    @Override
    public void onStreamChanged(StreamDesc streamDesc) {
        if (!streamProxyConcurrentMap.containsKey(streamDesc.getStreamId())) {
            LOGGER.warn("Updating non-existing stream proxy {}", streamDesc.getStreamId());
            this.onStreamAdded(streamDesc);
            return;
        }
        LOGGER.info("Updating stream proxy {}", streamDesc.getStreamId());
        try {
            LOGGER.info("Closing old stream proxy {}", streamDesc.getStreamId());
            streamProxyConcurrentMap.get(streamDesc.getStreamId()).close();
        } catch (IOException e) {
            LOGGER.error("Unable to close {}", streamProxyConcurrentMap.get(streamDesc.getStreamId()));
        } finally {
            LOGGER.info("Adding stream proxy {}", streamDesc.getStreamId());
            StreamProxyImpl proxy = new StreamProxyImpl();
            proxy.open(streamDesc);
            streamProxyConcurrentMap.put(streamDesc.getStreamId(), proxy);
        }
    }

    @Override
    public void onStreamRemoved(StreamDesc streamDesc) {
        LOGGER.info("Removing stream proxy {}", streamDesc.getStreamId());
        if (streamProxyConcurrentMap.containsKey(streamDesc.getStreamId())) {
            try {
                streamProxyConcurrentMap.get(streamDesc.getStreamId()).close();
            } catch (IOException e) {
                LOGGER.error("Unable to close {}", streamProxyConcurrentMap.get(streamDesc.getStreamId()));
            }
        } else {
            LOGGER.warn("Unable to remove stream proxy {}, because not exist", streamDesc.getStreamId());
        }
    }
}