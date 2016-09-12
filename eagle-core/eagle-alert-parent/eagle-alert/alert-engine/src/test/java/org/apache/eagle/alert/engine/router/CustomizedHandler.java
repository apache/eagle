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
package org.apache.eagle.alert.engine.router;

import org.apache.eagle.alert.engine.Collector;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.PolicyHandlerContext;
import org.apache.eagle.alert.engine.evaluator.PolicyStreamHandler;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;

import java.util.Map;

/**
 * Created on 8/29/16.
 */
public class CustomizedHandler implements PolicyStreamHandler {
    private Collector<AlertStreamEvent> collector;

    public CustomizedHandler(Map<String, StreamDefinition> sds) {
    }

    @Override
    public void prepare(Collector<AlertStreamEvent> collector, PolicyHandlerContext context) throws Exception {
        this.collector = collector;
    }

    @Override
    public void send(StreamEvent event) throws Exception {
        this.collector.emit(new AlertStreamEvent());
    }

    @Override
    public void close() throws Exception {

    }
}
