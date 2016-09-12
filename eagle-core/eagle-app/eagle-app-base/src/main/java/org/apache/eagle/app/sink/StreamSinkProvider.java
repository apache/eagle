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

import org.apache.eagle.metadata.model.StreamSinkConfig;
import com.typesafe.config.Config;

import java.lang.reflect.ParameterizedType;

public interface StreamSinkProvider<S extends StreamSink<D>, D extends StreamSinkConfig> {
    D getSinkConfig(String streamId, Config config);

    S getSink();

    default S getSink(String streamId, Config config) {
        S s = getSink();
        s.init(streamId, getSinkConfig(streamId, config));
        return s;
    }

    default Class<? extends S> getSinkType() {
        return (Class<S>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    default Class<? extends D> getSinkConfigType() {
        return (Class<D>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1];
    }
}
