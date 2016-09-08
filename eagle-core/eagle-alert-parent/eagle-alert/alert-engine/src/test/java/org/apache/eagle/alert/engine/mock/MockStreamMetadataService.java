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
package org.apache.eagle.alert.engine.mock;

import java.util.HashMap;
import java.util.Map;

import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinitionNotFoundException;

public class MockStreamMetadataService {
    private final Map<String, StreamDefinition> streamSchemaMap = new HashMap<>();

    public StreamDefinition getStreamDefinition(String streamId) throws StreamDefinitionNotFoundException {
        if (streamSchemaMap.containsKey(streamId)) {
            return streamSchemaMap.get(streamId);
        } else {
            throw new StreamDefinitionNotFoundException(streamId);
        }
    }

    public void registerStream(String streamId, StreamDefinition schema) {
        streamSchemaMap.put(streamId, schema);
    }
}