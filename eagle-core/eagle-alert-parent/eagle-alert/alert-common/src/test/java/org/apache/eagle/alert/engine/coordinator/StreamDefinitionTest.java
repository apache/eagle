/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.alert.engine.coordinator;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class StreamDefinitionTest {
    @Test
    public void testStreamDefinition() {

        List<StreamColumn> streamColumns = new ArrayList<>();
        streamColumns.add(new StreamColumn.Builder().name("name").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("host").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("flag").type(StreamColumn.Type.BOOL).build());
        streamColumns.add(new StreamColumn.Builder().name("data").type(StreamColumn.Type.LONG).build());
        streamColumns.add(new StreamColumn.Builder().name("value").type(StreamColumn.Type.DOUBLE).build());

        StreamDefinition streamDefinition = new StreamDefinition();
        Assert.assertEquals("StreamDefinition[streamId=null, dataSource=null, description=null, validate=false, timeseries=false, columns=[]", streamDefinition.toString());
        streamDefinition.setColumns(streamColumns);

        Assert.assertEquals(3, streamDefinition.getColumnIndex("data"));
        Assert.assertEquals(-1, streamDefinition.getColumnIndex("DATA"));
        Assert.assertEquals(-1, streamDefinition.getColumnIndex("isYhd"));
        Assert.assertEquals("StreamDefinition[streamId=null, dataSource=null, description=null, validate=false, timeseries=false, columns=[StreamColumn=name[name], type=[string], defaultValue=[null], required=[false], nodataExpression=[null], StreamColumn=name[host], type=[string], defaultValue=[null], required=[false], nodataExpression=[null], StreamColumn=name[flag], type=[bool], defaultValue=[null], required=[false], nodataExpression=[null], StreamColumn=name[data], type=[long], defaultValue=[null], required=[false], nodataExpression=[null], StreamColumn=name[value], type=[double], defaultValue=[null], required=[false], nodataExpression=[null]]", streamDefinition.toString());
        StreamDefinition streamDefinition1 = streamDefinition.copy();
        Assert.assertEquals("StreamDefinition[streamId=null, dataSource=null, description=null, validate=false, timeseries=false, columns=[StreamColumn=name[name], type=[string], defaultValue=[null], required=[false], nodataExpression=[null], StreamColumn=name[host], type=[string], defaultValue=[null], required=[false], nodataExpression=[null], StreamColumn=name[flag], type=[bool], defaultValue=[null], required=[false], nodataExpression=[null], StreamColumn=name[data], type=[long], defaultValue=[null], required=[false], nodataExpression=[null], StreamColumn=name[value], type=[double], defaultValue=[null], required=[false], nodataExpression=[null]]", streamDefinition1.toString());

        Assert.assertTrue(streamDefinition1.equals(streamDefinition));
        Assert.assertFalse(streamDefinition1 == streamDefinition);
        Assert.assertTrue(streamDefinition1.hashCode() == streamDefinition.hashCode());
    }
}
