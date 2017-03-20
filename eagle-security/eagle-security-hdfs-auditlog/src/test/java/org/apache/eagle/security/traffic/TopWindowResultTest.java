/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.security.traffic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.security.auditlog.TopWindowResult;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class TopWindowResultTest {

    @Test
    public void test() {
        ObjectMapper objectMapper = new ObjectMapper();
        TopWindowResult result = null;
        String data2 = "{\"timestamp\":\"2017-03-08T00:29:33-0700\",\"windows\":[{\"windowLenMs\":60000,\"ops\":[]},{\"windowLenMs\":300000,\"ops\":[]},{\"windowLenMs\":1500000,\"ops\":[]}]}";
        try {
            result = objectMapper.readValue(data2, TopWindowResult.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(result != null);
        Assert.assertTrue(result.getWindows().size() == 3);
    }

    @Test
    public void testTime() {
        String time = "2017-03-07T21:36:51-0700";
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

        try {
            long t1 = df.parse(time).getTime();
            String time2 = "2017-03-07 21:36:51";
            long t2 = DateTimeUtil.humanDateToSeconds(time2, TimeZone.getTimeZone("GMT-7")) * 1000;
            Assert.assertTrue(t1 == t2);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
