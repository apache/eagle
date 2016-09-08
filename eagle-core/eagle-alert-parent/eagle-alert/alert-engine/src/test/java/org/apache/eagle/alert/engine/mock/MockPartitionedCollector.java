package org.apache.eagle.alert.engine.mock;

import org.apache.eagle.alert.engine.PartitionedEventCollector;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

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
public class MockPartitionedCollector implements PartitionedEventCollector {
    @SuppressWarnings("unused")
    private final static Logger LOG = LoggerFactory.getLogger(MockPartitionedCollector.class);
    private List<PartitionedEvent> cache;

    public MockPartitionedCollector() {
        cache = new LinkedList<>();
    }

    public void emit(PartitionedEvent event) {
        cache.add(event);
    }

    public void clear() {
        cache.clear();
    }

    public List<PartitionedEvent> get() {
        return cache;
    }

    public int size() {
        return cache.size();
    }

    @Override
    public void drop(PartitionedEvent event) {

    }
}