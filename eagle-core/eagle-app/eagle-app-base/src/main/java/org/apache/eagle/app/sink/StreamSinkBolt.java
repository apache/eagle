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
package org.apache.eagle.app.sink;

import backtype.storm.topology.base.BaseBasicBolt;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.app.ApplicationContainer;
import org.apache.eagle.app.ApplicationLifecycle;
import org.apache.eagle.app.sink.mapper.StreamEventMapper;
import org.apache.eagle.metadata.model.StreamSinkDesc;

public abstract class StreamSinkBolt<T extends StreamSinkDesc> extends BaseBasicBolt implements ApplicationLifecycle {
    /**
     * Should only initialize metadata in this method but must not open any resource like connection
     *
     * @param streamDefinition
     * @param context
     */
    public abstract T init(StreamDefinition streamDefinition, ApplicationContainer context);

    public abstract StreamSinkBolt<T> setEventMapper(StreamEventMapper streamEventMapper);

    @Override
    public void onStart() {
        // StreamSinkBolt by default will do nothing when application start
    }

    @Override
    public void onStop() {
        // StreamSinkBolt by default will do nothing when application start
    }
}