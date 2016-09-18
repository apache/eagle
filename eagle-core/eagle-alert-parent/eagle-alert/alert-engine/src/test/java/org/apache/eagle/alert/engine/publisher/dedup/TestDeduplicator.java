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
package org.apache.eagle.alert.engine.publisher.dedup;

import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.impl.EventUniq;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Ignore
public class TestDeduplicator extends ExtendedDeduplicator {

    public TestDeduplicator(Config config, Map<String, String> properties, List<String> customDedupFields,
			String dedupStateField, DedupCache dedupCache) {
		super(config, properties, customDedupFields, dedupStateField, dedupCache);
	}

	private static final Logger LOG = LoggerFactory.getLogger(TestDeduplicator.class);

    @Override
    public List<AlertStreamEvent> dedup(AlertStreamEvent event) {
        StreamDefinition streamDefinition = event.getSchema();
        HashMap<String, String> customFieldValues = new HashMap<>();
        String stateFiledValue = null;
        for (int i = 0; i < event.getData().length; i++) {
            if (i > streamDefinition.getColumns().size()) {
                continue;
            }
            String colName = streamDefinition.getColumns().get(i).getName();

            if (colName.equals(this.getDedupStateField())) {
                stateFiledValue = event.getData()[i].toString();
            }

            // make all of the field as unique key if no custom dedup field provided
            if (this.getCustomDedupFields() == null || this.getCustomDedupFields().size() <= 0) {
                customFieldValues.put(colName, event.getData()[i].toString());
            } else {
                for (String field : this.getCustomDedupFields()) {
                    if (colName.equals(field)) {
                        customFieldValues.put(field, event.getData()[i].toString());
                        break;
                    }
                }
            }
        }
        LOG.info("event: " + event);
        EventUniq eventkey = new EventUniq(event.getStreamId(), event.getPolicyId(), event.getCreatedTime(), customFieldValues);
        LOG.info("event key: " + eventkey);
        LOG.info("dedup field: " + this.getDedupStateField());
        LOG.info("dedup value: " + stateFiledValue);
        List<AlertStreamEvent> result = this.getDedupCache().dedup(event, eventkey, this.getDedupStateField(), stateFiledValue);
        return result;
    }

    @Override
    public void setDedupIntervalMin(String intervalMin) {
    }

}
