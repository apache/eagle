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
package org.apache.eagle.security.crawler.audit;

import org.apache.eagle.security.hdfs.MAPRFSAuditLogObject;
import org.apache.eagle.security.hdfs.MAPRFSAuditLogParser;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestMAPRFSAuditLogParser {

    private final static Logger LOG = LoggerFactory.getLogger(TestMAPRFSAuditLogParser.class);
    @Test
    public void testMAPRParser() {
        String log = "{\"timestamp\":{\"$date\":\"2015-06-06T10:44:22.800Z\"},\"operation\":\"MKDIR\",\"uid\":0,\"ipAddress\":\n" +
                "\"10.10.104.51\",\"parentFid\":\"2049.51.131248\",\"childFid\":\"2049.56.131258\",\"childName\":\n" +
                "\"ycsbTmp_1433587462796\",\"volumeId\":68048396,\"status\":0}";
        MAPRFSAuditLogParser parser = new MAPRFSAuditLogParser();
        MAPRFSAuditLogObject entity = new MAPRFSAuditLogObject();
        try {
            entity = parser.parse(log);
        }catch (Exception e){
            LOG.info(e.getMessage());
        }

        Assert.assertEquals("MKDIR",entity.cmd);
        Assert.assertEquals("0",entity.user);
        Assert.assertEquals("10.10.104.51",entity.host);
        Assert.assertEquals("null",entity.src);
        Assert.assertEquals("null",entity.dst);
        Assert.assertEquals("0", entity.status);

        log = "{\"timestamp\":{\"$date\":\"2016-02-19T01:50:01.962Z\"},\"operation\":\"LOOKUP\",\"uid\":5000,\"ipAddress\":\"192.168.6.148\",\"srcFid\":\"2049.40.131192\",\"dstFid\":\"2049.1032.133268\",\"srcName\":\"share\",\"volumeId\":186635570,\"status\":0}\n";
        try {
            entity = parser.parse(log);
        }catch (Exception e){
            LOG.info(e.getMessage());
        }
        Assert.assertEquals("LOOKUP",entity.cmd);
        Assert.assertEquals("5000",entity.user);
        Assert.assertEquals("192.168.6.148",entity.host);
        Assert.assertEquals("2049.40.131192",entity.src);
        Assert.assertEquals("2049.1032.133268",entity.dst);
        Assert.assertEquals("0", entity.status);
    }
}