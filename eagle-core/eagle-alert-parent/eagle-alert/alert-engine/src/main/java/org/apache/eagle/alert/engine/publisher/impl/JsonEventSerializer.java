/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.publisher.impl;

import org.apache.eagle.alert.engine.codec.IEventSerializer;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @since Jul 9, 2016
 *
 */
public class JsonEventSerializer implements IEventSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(JsonEventSerializer.class);

    @SuppressWarnings("rawtypes")
    public JsonEventSerializer(Map stormConf) throws Exception {
    }

    @Override
    public Object serialize(AlertStreamEvent event) {
        String result = streamEventToJson(event);
        if (LOG.isDebugEnabled()) {
            LOG.debug("serialized alert event : {}", result);
        }
        return result;
    }

    public String streamEventToJson(AlertStreamEvent event) {
        Map<String, Object> jsonMap = new HashMap<String, Object>();
        jsonMap.put("policyId", event.getPolicyId());
        jsonMap.put("streamId", event.getStreamId());
        jsonMap.put("createBy", event.getCreatedBy());
        jsonMap.put("createTime", event.getCreatedTime());
        // data
        int size = event.getData().length;
        List<StreamColumn> columns = event.getSchema().getColumns();
        for (int i = 0; i < size; i++) {
            if (columns.size() < i) {
                // redundant check to log inconsistency
                LOG.error(" stream event data have different lenght compare to column definition! ");
            } else {
                jsonMap.put(columns.get(i).getName(), event.getData()[i]);
            }
        }
        return JsonUtils.writeValueAsString(jsonMap);
    }

}