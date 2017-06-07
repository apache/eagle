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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class DedupEntity {

    private String publishName;
    private EventUniq eventEniq;
    private List<DedupValue> dedupValues = new ArrayList<DedupValue>();

    public DedupEntity(String publishName, EventUniq eventEniq, ConcurrentLinkedDeque<DedupValue> dedupValues) {
        this.publishName = publishName;
        this.eventEniq = eventEniq;
        this.dedupValues.addAll(dedupValues);
    }

    public DedupEntity(String publishName, EventUniq eventEniq, List<DedupValue> dedupValues) {
        this.publishName = publishName;
        this.eventEniq = eventEniq;
        this.dedupValues = dedupValues;
    }

    public String getPublishName() {
        return publishName;
    }

    public void setPublishName(String publishName) {
        this.publishName = publishName;
    }

    public EventUniq getEventEniq() {
        return eventEniq;
    }

    public void setEventEniq(EventUniq eventEniq) {
        this.eventEniq = eventEniq;
    }

    public List<DedupValue> getDedupValues() {
        return dedupValues;
    }

    public void setDedupValues(List<DedupValue> dedupValues) {
        this.dedupValues = dedupValues;
    }

    public ConcurrentLinkedDeque<DedupValue> getDedupValuesInConcurrentLinkedDeque() {
        ConcurrentLinkedDeque<DedupValue> result = new ConcurrentLinkedDeque<DedupValue>();
        result.addAll(this.getDedupValues());
        return result;
    }

}
