package org.apache.eagle.alert.engine.evaluator;

import java.io.Serializable;

import org.apache.eagle.alert.engine.AlertStreamCollector;
import org.apache.eagle.alert.engine.StreamContext;
import org.apache.eagle.alert.engine.model.PartitionedEvent;

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

/**
 * policy group refers to the policies which belong to the same MonitoredStream
 * 3 lifecycle steps are involved in PolicyGroupEvaluator
 * Step 1: create object. Be aware that in distributed environment, this object may be serialized and transferred across network
 * Step 2: init. This normally is invoked only once before nextEvent is invoked
 * Step 3: nextEvent
 * Step 4: close
 */
public interface PolicyGroupEvaluator extends PolicyChangeListener, Serializable {
    void init(StreamContext context, AlertStreamCollector collector);

    /**
     * Evaluate event
     */
    void nextEvent(PartitionedEvent event);

    String getName();

    void close();
}