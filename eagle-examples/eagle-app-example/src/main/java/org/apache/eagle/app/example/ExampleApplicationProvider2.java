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
package org.apache.eagle.app.example;

import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.app.spi.AbstractApplicationProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Define application provider with metadata.xml
 */
public class ExampleApplicationProvider2 extends AbstractApplicationProvider<ExampleStormApplication> {
    @Override
    protected void configure() {
        setType("EXAMPLE_APPLICATION_2");
        setName("Example Monitoring Application 2");
        setVersion("0.5.0-incubating");
        setAppClass(ExampleStormApplication.class);
        setViewPath("/apps/example");
        setAppConfig("ExampleApplicationConf.xml");
        setStreams(Arrays.asList(createSampleStreamDefinition("SAMPLE_STREAM_1"), createSampleStreamDefinition("SAMPLE_STREAM_2")));
    }

    private static StreamDefinition createSampleStreamDefinition(String streamId){
        StreamDefinition sampleStreamDefinition = new StreamDefinition();
        sampleStreamDefinition.setStreamId(streamId);
        sampleStreamDefinition.setTimeseries(true);
        sampleStreamDefinition.setValidate(true);
        sampleStreamDefinition.setDescription("Auto generated sample Schema for "+streamId);
        List<StreamColumn> streamColumns = new ArrayList<>();

        streamColumns.add(new StreamColumn.Builder().name("metric").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("source").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("timestamp").type(StreamColumn.Type.LONG).build());
        streamColumns.add(new StreamColumn.Builder().name("value").type(StreamColumn.Type.DOUBLE).build());
        sampleStreamDefinition.setColumns(streamColumns);
        return sampleStreamDefinition;
    }

    @Override
    public ExampleStormApplication getApplication() {
        return new ExampleStormApplication();
    }
}