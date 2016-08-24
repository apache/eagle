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

package org.apache.eagle.alert.engine.evaluator.impl;

import org.apache.eagle.alert.engine.AlertStreamCollector;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;

import scala.Tuple2;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class AlertBoltOutputCollectorSparkWrapper implements AlertStreamCollector {
    private final LinkedList<Tuple2<String, AlertStreamEvent>> collector = new LinkedList<>();

    public AlertBoltOutputCollectorSparkWrapper() {
    }

    @Override
    public void emit(AlertStreamEvent event) {
        collector.add(new Tuple2(event.getStreamId(), event));
    }

    public List<Tuple2<String, AlertStreamEvent>> emitResult() {
        if (collector.isEmpty()) {
            return Collections.emptyList();
        }
        LinkedList<Tuple2<String, AlertStreamEvent>> result = new LinkedList<>();
        result.addAll(collector);
        return result;
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {

    }
}
