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
import org.apache.eagle.app.AbstractApplicationProvider;
import org.apache.eagle.metadata.model.ApplicationDesc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExampleApplicationProvider extends AbstractApplicationProvider<ExampleApplication> {
    @Override
    protected void declare() {
        setType("EXAMPLE_APPLICATION");
        setName("Example Monitoring Application");
        setVersion("0.5.0-incubating");
        setAppClass(ExampleApplication.class);
        setViewPath("/apps/example");
        setAppConfig("ExampleApplicationConf.xml");
        setStreams(Arrays.asList(createSampleStreamDefinition("SAMPLE_STREAM_1"), createSampleStreamDefinition("SAMPLE_STREAM_2")));
    }

    private static StreamDefinition createSampleStreamDefinition(String streamId){
        StreamDefinition sampleStreamDefinition = new StreamDefinition();
        sampleStreamDefinition.setStreamId(streamId);
        sampleStreamDefinition.setTimeseries(true);
        sampleStreamDefinition.setValidate(true);
        sampleStreamDefinition.setDescription("Schema for "+streamId);
        List<StreamColumn> streamColumns = new ArrayList<>();

        streamColumns.add(new StreamColumn.Builder().name("name").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("host").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("flag").type(StreamColumn.Type.BOOL).build());
        streamColumns.add(new StreamColumn.Builder().name("timestamp").type(StreamColumn.Type.LONG).build());
        streamColumns.add(new StreamColumn.Builder().name("value").type(StreamColumn.Type.DOUBLE).build());
        sampleStreamDefinition.setColumns(streamColumns);
        return sampleStreamDefinition;
    }

    @Override
    public ExampleApplication getApplication() {
        return new ExampleApplication();
    }
}