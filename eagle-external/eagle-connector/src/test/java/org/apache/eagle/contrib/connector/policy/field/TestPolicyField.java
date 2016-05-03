/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */
package org.apache.eagle.contrib.connector.policy.field;

import org.junit.Assert;
import org.junit.Test;


public class TestPolicyField {

    @Test
    public void testCommonField() throws Exception {
        CommonField commonField = new CommonField("src");
        commonField.equalsTo("/test0");
        Assert.assertEquals(commonField.getSiddhiExpression().trim(),"(src == '/test0')");
        commonField.notEqualsTo("/test1");
        Assert.assertEquals(commonField.getSiddhiExpression().trim(),"(src == '/test0' or src != '/test1')");
        commonField.contains("/test2");
        Assert.assertEquals(commonField.getSiddhiExpression().trim(),"(src == '/test0' or src != '/test1' or str:contains(src,'/test2')==true)");
        commonField.regexp(".*test3.*");
        Assert.assertEquals(commonField.getSiddhiExpression().trim(),"(src == '/test0' or src != '/test1' or str:contains(src,'/test2')==true or str:regexp(src,'.*test3.*')==true)");
    }

    @Test
    public void testAllowedField(){
        AllowedField allowedField = new AllowedField();
        allowedField.setAllowed(true);
        Assert.assertEquals("(allowed == true)", allowedField.getSiddhiExpression());

        allowedField.setAllowed(false);
        Assert.assertEquals("(allowed == false)", allowedField.getSiddhiExpression());

    }

    @Test
    public void testTimestampField(){
        TimestampField timestampField = new TimestampField();
        timestampField.equalsTo(1);
        Assert.assertEquals(timestampField.getSiddhiExpression(),"(timestamp == 1)");
        timestampField.notEqualsTo(2);
        Assert.assertEquals(timestampField.getSiddhiExpression(),"(timestamp == 1 or timestamp != 2)");
        timestampField.largerThan(3);
        Assert.assertEquals(timestampField.getSiddhiExpression(),"(timestamp == 1 or timestamp != 2 or timestamp > 3)");
        timestampField.largerThanOrEqualTo(4);
        Assert.assertEquals(timestampField.getSiddhiExpression(),"(timestamp == 1 or timestamp != 2 or timestamp > 3 or timestamp >= 4)");
        timestampField.lessThan(5);
        Assert.assertEquals(timestampField.getSiddhiExpression(),"(timestamp == 1 or timestamp != 2 or timestamp > 3 or timestamp >= 4 or timestamp < 5)");
        timestampField.lessThanOrEqualTo(6);
        Assert.assertEquals(timestampField.getSiddhiExpression(),"(timestamp == 1 or timestamp != 2 or timestamp > 3 or timestamp >= 4 or timestamp < 5 or timestamp <= 6)");

    }
}