/*
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
package org.apache.eagle.app;

import com.typesafe.config.Config;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.app.sink.StreamSink;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.StreamDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Application Execution Context
 */
public class ApplicationContext implements Serializable {
    private final Config envConfig;
    private final ApplicationEntity appEntity;
    private final static Logger LOG = LoggerFactory.getLogger(ApplicationContext.class);
    private final Map<String, StreamSink> streamSinkMap;
    private final Map<String,StreamDesc> streamDescMap;

    public ApplicationContext(ApplicationEntity appEntity, Config envConfig){
        this.appEntity = appEntity;
        this.envConfig = envConfig;
        this.streamSinkMap = new HashMap<>();
        this.streamDescMap = new HashMap<>();

        // TODO: Decouple out of ApplicationContext constructor
        doInit();
    }

    private void doInit() {
        try {
            Class<?> sinkClass = appEntity.getDescriptor().getSinkClass();
            List<StreamDefinition> outputStreams = appEntity.getDescriptor().getStreams();
            if(null != outputStreams){
                Constructor constructor = sinkClass.getConstructor(StreamDefinition.class,ApplicationContext.class);
                outputStreams.forEach((stream) -> {
                    try {
                        StreamSink streamSink = (StreamSink) constructor.newInstance(stream,this);
                        streamSinkMap.put(stream.getStreamId(), streamSink);
                        StreamDesc streamDesc = new StreamDesc();
                        streamDesc.setStreamSchema(stream);
                        streamDesc.setSinkContext(streamSink.getSinkContext());
                        streamDesc.setSinkType(sinkClass);
                        streamDesc.setStreamId(stream.getStreamId());
                        streamDescMap.put(streamDesc.getStreamId(),streamDesc);
                    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                        LOG.error("Failed to initialize instance "+sinkClass.getCanonicalName()+" for application: {}",this.getAppEntity());
                        throw new RuntimeException("Failed to initialize instance "+sinkClass.getCanonicalName()+" for application:"+this.getAppEntity(),e);
                    }
                });
            }
        } catch (NoSuchMethodException e) {
            LOG.error(e.getMessage(),e);
            throw new RuntimeException(e.getMessage(),e.getCause());
        }
    }

    public ApplicationEntity getAppEntity() {
        return appEntity;
    }

    public Config getEnvConfig() {
        return envConfig;
    }

    /**
     * Make sure streamId is declared in Application Providers
     *
     * @param streamId
     * @return
     */
    public StreamSink getStreamSink(String streamId){
        if(streamSinkMap.containsKey(streamId)) {
            return streamSinkMap.get(streamId);
        } else {
            throw new IllegalStateException("Stream (ID: "+streamId+") was not declared in "+this.appEntity.getDescriptor().getProviderClass().getCanonicalName()+" before being called");
        }
    }

    public Collection<StreamDesc> getStreamSinkDescs(){
        return streamDescMap.values();
    }
}