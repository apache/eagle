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
package org.apache.eagle.security.hbase;

import org.apache.eagle.security.hbase.parse.HbaseAuditLogObject;
import org.apache.eagle.security.hbase.parse.HbaseAuditLogParser;
import org.junit.Assert;
import org.junit.Test;


public class TestHbaseAuditLogParser {
    HbaseAuditLogParser parser = new HbaseAuditLogParser();

    @Test
    public void test() throws Exception {
        String log = "2015-08-11 13:31:03,729 TRACE SecurityLogger.org.apache.hadoop.hbase.security.access.AccessController: Access allowed for user eagle; reason: Table permission granted; remote address: /127.0.0.1; request: get; context: (user=eagle,scope=hbase:namespace,family=info, action=READ)";
        HbaseAuditLogObject obj = parser.parse(log);
        Assert.assertEquals(obj.action, "READ");
        Assert.assertEquals(obj.host, "/127.0.0.1");
        Assert.assertEquals(obj.scope, "hbase:namespace");
    }

    @Test
    public void test2() throws Exception {
        String log = "2015-08-04 12:29:03,073 TRACE SecurityLogger.org.apache.hadoop.hbase.security.access.AccessController: Access allowed for user eagle; reason: Global check allowed; remote address: ; request: preOpen; context: (user=eagle, scope=GLOBAL, family=, action=ADMIN)";
        HbaseAuditLogObject obj = parser.parse(log);
        Assert.assertEquals(obj.action, "ADMIN");
        Assert.assertEquals(obj.host, "");
        Assert.assertEquals(obj.scope, "GLOBAL");
    }
}
