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
package org.apache.eagle.app.example;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.eagle.app.ApplicationContext;
import org.apache.eagle.app.AbstractApplication;

public class ExampleApplication extends AbstractApplication {
    protected void buildTopology(TopologyBuilder builder, ApplicationContext context) {
        builder.setSpout("mockMetricSpout", new RandomEventSpout(), 4);
        builder.setBolt("sink_1",context.getStreamSink("SAMPLE_STREAM_1")).fieldsGrouping("mockMetricSpout",new Fields("key"));
        builder.setBolt("sink_2",context.getStreamSink("SAMPLE_STREAM_2")).fieldsGrouping("mockMetricSpout",new Fields("key"));
    }
}