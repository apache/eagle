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
package org.apache.eagle.app.messaging;

import com.typesafe.config.Config;
import org.apache.eagle.metadata.model.StreamSinkConfig;
import org.apache.eagle.metadata.model.StreamSourceConfig;

import java.lang.reflect.ParameterizedType;

/**
 * Stream Messaging Bus.
 */
public interface StreamProvider<W extends StreamSink<C>, C extends StreamSinkConfig,
        R extends StreamSource<F>, F extends StreamSourceConfig> {

    C getSinkConfig(String streamId, Config config);

    W getSink();

    default W getSink(String streamId, Config config) {
        W s = getSink();
        s.init(streamId, getSinkConfig(streamId, config));
        return s;
    }

    F getSourceConfig(String streamId, Config config);

    R getSource();

    default R getSource(String streamId, Config config) {
        R i = getSource();
        i.init(streamId, getSourceConfig(streamId, config));
        return i;
    }
}