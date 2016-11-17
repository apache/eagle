/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.util;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JSONUtilsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testGetString() throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse("{\"id\":\"application_1479206441898_30784\",\"isYhd\":\"pms\"}");
        Assert.assertEquals("application_1479206441898_30784", JSONUtils.getString(jsonObject, "id"));
    }

    @Test
    public void testGetBoolean() throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse("{\"id\":\"application_1479206441898_30784\",\"isYhd\":true}");
        Assert.assertEquals(Boolean.TRUE, JSONUtils.getBoolean(jsonObject, "isYhd"));
        jsonObject = (JSONObject) parser.parse("{\"id\":\"application_1479206441898_30784\",\"isYhd\":false}");
        Assert.assertEquals(Boolean.FALSE, JSONUtils.getBoolean(jsonObject, "isYhd"));
    }

    @Test
    public void testGetLong() throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse("{\"salary\":2000,\"isYhd\":\"false\"}");
        Assert.assertEquals(2000, JSONUtils.getLong(jsonObject, "salary"));
    }

    @Test
    public void testGetLong1() throws ParseException {
        thrown.expect(ClassCastException.class);
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse("{\"salary\":\"2000\",\"isYhd\":\"false\"}");
        Assert.assertEquals(2000, JSONUtils.getLong(jsonObject, "salary"));
    }


    @Test
    public void testGetInt() throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse("{\"salary\":\"2000\",\"isYhd\":\"false\"}");
        Assert.assertEquals(2000, JSONUtils.getInt(jsonObject, "salary"));
    }

    @Test
    public void testGetInt1() throws ParseException {
        thrown.expect(org.json.JSONException.class);
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse("{\"salary\":\"xxx\",\"isYhd\":\"false\"}");
        Assert.assertEquals(2000, JSONUtils.getInt(jsonObject, "salary"));
    }
}
