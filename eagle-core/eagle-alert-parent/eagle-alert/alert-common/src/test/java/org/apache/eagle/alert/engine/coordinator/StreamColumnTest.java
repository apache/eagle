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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;


public class StreamColumnTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testStreamStringColumn() {
        StreamColumn streamColumn = new StreamColumn.Builder().name("NAMEyhd").type(StreamColumn.Type.STRING).defaultValue("EAGLEyhd").required(true).build();
        streamColumn.setNodataExpression("PT1M,dynamic,1,NAMEyhd");
        Assert.assertEquals("StreamColumn=name[NAMEyhd], type=[string], defaultValue=[EAGLEyhd], required=[true], nodataExpression=[PT1M,dynamic,1,NAMEyhd]", streamColumn.toString());
        Assert.assertTrue(streamColumn.getDefaultValue() instanceof String);
    }

    @Test
    public void testStreamLongColumn() {
        thrown.expect(NumberFormatException.class);
        new StreamColumn.Builder().name("salary").type(StreamColumn.Type.LONG).defaultValue("eagle").required(true).build();
    }

    @Test
    public void testStreamLongColumn1() {
        StreamColumn streamColumn = new StreamColumn.Builder().name("salary").type(StreamColumn.Type.LONG).defaultValue("0").required(true).build();
        streamColumn.setNodataExpression("PT1M,dynamic,1,salary");
        Assert.assertEquals("StreamColumn=name[salary], type=[long], defaultValue=[0], required=[true], nodataExpression=[PT1M,dynamic,1,salary]", streamColumn.toString());
        Assert.assertTrue(streamColumn.getDefaultValue() instanceof Long);
    }

    @Test
    public void testStreamDoubleColumn() {
        thrown.expect(NumberFormatException.class);
        new StreamColumn.Builder().name("salary").type(StreamColumn.Type.DOUBLE).defaultValue("eagle").required(true).build();
    }

    @Test
    public void testStreamDoubleColumn1() {
        StreamColumn streamColumn = new StreamColumn.Builder().name("salary").type(StreamColumn.Type.DOUBLE).defaultValue("0.1").required(true).build();
        streamColumn.setNodataExpression("PT1M,dynamic,1,salary");
        Assert.assertEquals("StreamColumn=name[salary], type=[double], defaultValue=[0.1], required=[true], nodataExpression=[PT1M,dynamic,1,salary]", streamColumn.toString());

        streamColumn = new StreamColumn.Builder().name("salary").type(StreamColumn.Type.DOUBLE).defaultValue("-0.1").required(true).build();
        streamColumn.setNodataExpression("PT1M,dynamic,1,salary");
        Assert.assertEquals("StreamColumn=name[salary], type=[double], defaultValue=[-0.1], required=[true], nodataExpression=[PT1M,dynamic,1,salary]", streamColumn.toString());

        streamColumn = new StreamColumn.Builder().name("salary").type(StreamColumn.Type.DOUBLE).defaultValue("1").required(true).build();
        streamColumn.setNodataExpression("PT1M,dynamic,1,salary");
        Assert.assertEquals("StreamColumn=name[salary], type=[double], defaultValue=[1.0], required=[true], nodataExpression=[PT1M,dynamic,1,salary]", streamColumn.toString());
        Assert.assertTrue(streamColumn.getDefaultValue() instanceof Double);
    }

    @Test
    public void testStreamFloatColumn() {
        thrown.expect(NumberFormatException.class);
        new StreamColumn.Builder().name("salary").type(StreamColumn.Type.FLOAT).defaultValue("eagle").required(true).build();
    }

    @Test
    public void testStreamFloatColumn1() {
        StreamColumn streamColumn = new StreamColumn.Builder().name("salary").type(StreamColumn.Type.FLOAT).defaultValue("0.1").required(true).build();
        streamColumn.setNodataExpression("PT1M,dynamic,1,salary");
        Assert.assertEquals("StreamColumn=name[salary], type=[float], defaultValue=[0.1], required=[true], nodataExpression=[PT1M,dynamic,1,salary]", streamColumn.toString());

        streamColumn = new StreamColumn.Builder().name("salary").type(StreamColumn.Type.FLOAT).defaultValue("-0.1").required(true).build();
        streamColumn.setNodataExpression("PT1M,dynamic,1,salary");
        Assert.assertEquals("StreamColumn=name[salary], type=[float], defaultValue=[-0.1], required=[true], nodataExpression=[PT1M,dynamic,1,salary]", streamColumn.toString());

        streamColumn = new StreamColumn.Builder().name("salary").type(StreamColumn.Type.FLOAT).defaultValue("1").required(true).build();
        streamColumn.setNodataExpression("PT1M,dynamic,1,salary");
        Assert.assertEquals("StreamColumn=name[salary], type=[float], defaultValue=[1.0], required=[true], nodataExpression=[PT1M,dynamic,1,salary]", streamColumn.toString());
        Assert.assertTrue(streamColumn.getDefaultValue() instanceof Float);
    }

    @Test
    public void testStreamIntColumn() {
        thrown.expect(NumberFormatException.class);
        new StreamColumn.Builder().name("salary").type(StreamColumn.Type.INT).defaultValue("eagle").required(true).build();
    }

    @Test
    public void testStreamIntColumn1() {
        thrown.expect(NumberFormatException.class);
        new StreamColumn.Builder().name("salary").type(StreamColumn.Type.INT).defaultValue("0.1").required(true).build();
    }


    @Test
    public void testStreamIntColumn2() {
        StreamColumn streamColumn = new StreamColumn.Builder().name("salary").type(StreamColumn.Type.INT).defaultValue("1").required(true).build();
        streamColumn.setNodataExpression("PT1M,dynamic,1,salary");
        Assert.assertEquals("StreamColumn=name[salary], type=[int], defaultValue=[1], required=[true], nodataExpression=[PT1M,dynamic,1,salary]", streamColumn.toString());

        streamColumn = new StreamColumn.Builder().name("salary").type(StreamColumn.Type.INT).defaultValue("0").required(true).build();
        streamColumn.setNodataExpression("PT1M,dynamic,1,salary");
        Assert.assertEquals("StreamColumn=name[salary], type=[int], defaultValue=[0], required=[true], nodataExpression=[PT1M,dynamic,1,salary]", streamColumn.toString());
        Assert.assertTrue(streamColumn.getDefaultValue() instanceof Integer);
    }

    @Test
    public void testStreamBoolColumn() {
        StreamColumn streamBoolColumn = new StreamColumn.Builder().name("isYhd").type(StreamColumn.Type.BOOL).defaultValue("eagle").required(false).build();
        streamBoolColumn.setNodataExpression("PT1M,dynamic,1,isYhd");
        Assert.assertEquals("StreamColumn=name[isYhd], type=[bool], defaultValue=[false], required=[false], nodataExpression=[PT1M,dynamic,1,isYhd]", streamBoolColumn.toString());
        streamBoolColumn = new StreamColumn.Builder().name("isYhd").type(StreamColumn.Type.BOOL).defaultValue("1").required(true).build();
        streamBoolColumn.setNodataExpression("PT1M,dynamic,1,isYhd");
        Assert.assertEquals("StreamColumn=name[isYhd], type=[bool], defaultValue=[false], required=[true], nodataExpression=[PT1M,dynamic,1,isYhd]", streamBoolColumn.toString());
        streamBoolColumn = new StreamColumn.Builder().name("isYhd").type(StreamColumn.Type.BOOL).defaultValue("0").required(true).build();
        streamBoolColumn.setNodataExpression("PT1M,dynamic,1,isYhd");
        Assert.assertEquals("StreamColumn=name[isYhd], type=[bool], defaultValue=[false], required=[true], nodataExpression=[PT1M,dynamic,1,isYhd]", streamBoolColumn.toString());
        streamBoolColumn = new StreamColumn.Builder().name("isYhd").type(StreamColumn.Type.BOOL).defaultValue("True").required(true).build();
        streamBoolColumn.setNodataExpression("PT1M,dynamic,1,isYhd");
        Assert.assertEquals("StreamColumn=name[isYhd], type=[bool], defaultValue=[true], required=[true], nodataExpression=[PT1M,dynamic,1,isYhd]", streamBoolColumn.toString());
        Assert.assertTrue(streamBoolColumn.getDefaultValue() instanceof Boolean);
    }

    @Test
    public void testStreamObjectColumn() {
        thrown.expect(IllegalArgumentException.class);
        new StreamColumn.Builder().name("name").type(StreamColumn.Type.OBJECT).defaultValue("eagle").required(true).build();
    }

    @Test
    public void testStreamObjectColumn1() {
        StreamColumn streamColumn = new StreamColumn.Builder().name("name").type(StreamColumn.Type.OBJECT).defaultValue("{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}").required(true).build();
        streamColumn.setNodataExpression("PT1M,dynamic,1,name");
        Assert.assertEquals("StreamColumn=name[name], type=[object], defaultValue=[{name=heap.COMMITTED, Value=175636480}], required=[true], nodataExpression=[PT1M,dynamic,1,name]", streamColumn.toString());
        Assert.assertTrue(streamColumn.getDefaultValue() instanceof HashMap);
    }
}
