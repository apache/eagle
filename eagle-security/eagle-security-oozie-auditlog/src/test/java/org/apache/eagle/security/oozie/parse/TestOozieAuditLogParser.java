/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.eagle.security.oozie.parse;

import org.junit.Assert;
import org.junit.Test;


public class TestOozieAuditLogParser {
    OozieAuditLogParser parser = new OozieAuditLogParser();

    @Test
    public void testParser() throws Exception {


        String logline = "2016-04-27 15:01:14,526  INFO oozieaudit:520 - IP [192.168.7.199], USER [tangjijun], GROUP [pms], APP [My_Workflow], JOBID [0000000-160427140648764-oozie-oozi-W], " +
                "OPERATION [start], PARAMETER [0000000-160427140648764-oozie-oozi-W], STATUS [SUCCESS], HTTPCODE [200], ERRORCODE [501], ERRORMESSAGE [no problem]";
        OozieAuditLogObject obj = parser.parse(logline);
        Assert.assertEquals("192.168.7.199",obj.ip);
        Assert.assertEquals("tangjijun",obj.user);
        Assert.assertEquals("pms",obj.group);
        Assert.assertEquals("My_Workflow",obj.app);
        Assert.assertEquals("0000000-160427140648764-oozie-oozi-W",obj.jobId);
        Assert.assertEquals("start",obj.operation);
        Assert.assertEquals("0000000-160427140648764-oozie-oozi-W",obj.parameter);
        Assert.assertEquals("SUCCESS",obj.status);
        Assert.assertEquals("200",obj.httpcode);
        Assert.assertEquals("501",obj.errorcode);
        Assert.assertEquals("no problem",obj.errormessage);
        Assert.assertEquals("INFO",obj.level);
        Assert.assertEquals(1461769274526L,obj.timestamp);

    }
    @Test
    public void testParserNotMatch() throws Exception {
        String logline ="   2016-04-27 16:11:37,156  INFO oozieaudit:520 - Proxy user [hue] DoAs user [tangjijun] Request [http://zhangqihui:11000/oozie/v1/job/0000001-160427140648764-oozie-oozi-W?action=rerun&timezone=America%2FLos_Angeles&user.name=hue&doAs=tangjijun]";
        OozieAuditLogObject obj = parser.parse(logline);
        Assert.assertTrue(obj == null);
    }

}
