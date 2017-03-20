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
package org.apache.eagle.alert.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class StreamValidator {
    private final StreamDefinition streamDefinition;
    private final Map<String, StreamColumn> streamColumnMap;

    public StreamValidator(StreamDefinition streamDefinition) {
        this.streamDefinition = streamDefinition;
        this.streamColumnMap = new HashMap<>();
        for (StreamColumn column : this.streamDefinition.getColumns()) {
            streamColumnMap.put(column.getName(), column);
        }
    }

    public void validateMap(Map<String, Object> event) throws StreamValidationException {
        final List<String> errors = new LinkedList<>();
        this.streamDefinition.getColumns().forEach((column -> {
            if (column.isRequired() && !event.containsKey(column.getName())) {
                errors.add("[" + column.getName() + "]: required but absent");
            }
        }));
        for (Object eventKey : event.keySet()) {
            if (!streamColumnMap.containsKey(eventKey)) {
                errors.add("[" + eventKey + "]: invalid column");
            }
        }

        if (errors.size() > 0) {
            throw new StreamValidationException(errors.size() + " validation errors: " + StringUtils.join(errors.toArray(), "; "));
        }
    }
}
