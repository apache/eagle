/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.security.auditlog;

import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.security.hdfs.HDFSAuditLogObject;
import org.apache.eagle.security.hdfs.HDFSAuditLogParser;
import org.junit.Test;

import java.util.*;

public class TestUserCommandReassembler {
    private Map parseEvent(String log) throws Exception{
        HDFSAuditLogParser deserializer = new HDFSAuditLogParser();
        HDFSAuditLogObject entity = deserializer.parse(log);
        Map<String, Object> map = new TreeMap<String, Object>();
        map.put("src", entity.src);
        map.put("dst", entity.dst);
        map.put("host", entity.host);
        map.put("timestamp", entity.timestamp);
        map.put("allowed", entity.allowed);
        map.put("user", entity.user);
        map.put("cmd", entity.cmd);
        return map;
    }

    /**
     * 2015-11-19 23:57:02,934 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=getfileinfo src=/tmp/private dst=null perm=null proto=rpc
     * 2015-11-19 23:57:03,046 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=append src=/tmp/private dst=null perm=null proto=rpc
     * 2015-11-19 23:57:03,118 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=getfileinfo src=/tmp/private dst=null perm=null proto=rpc
     */
    @Test
    public void testAppend() throws Exception{
        String e1 = "2015-11-19 23:57:02,934 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=getfileinfo src=/tmp/private dst=null perm=null proto=rpc";
        String e2 = "2015-11-19 23:57:03,046 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=append src=/tmp/private dst=null perm=null proto=rpc";
        String e3 = "2015-11-19 23:57:03,118 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=getfileinfo src=/tmp/private dst=null perm=null proto=rpc";
        HdfsUserCommandReassembler assembler = new HdfsUserCommandReassembler();Config config = ConfigFactory.load();
        assembler.prepareConfig(config);
        assembler.init();

        OutputCollector collector = new OutputCollector(new IOutputCollector() {
            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                return null;
            }

            @Override
            public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {

            }

            @Override
            public void ack(Tuple input) {

            }

            @Override
            public void fail(Tuple input) {

            }

            @Override
            public void resetTimeout(Tuple tuple) {

            }

            @Override
            public void reportError(Throwable error) {

            }
        });
        assembler.flatMap(Arrays.asList("user1", parseEvent(e1)), collector);
        assembler.flatMap(Arrays.asList("user1", parseEvent(e2)), collector);
        assembler.flatMap(Arrays.asList("user1", parseEvent(e3)), collector);
        // sleep for a while for Siddhi engine callback to be invoked
        Thread.sleep(100);
    }

    /**
     * 2015-11-19 23:47:28,922 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=getfileinfo src=/tmp/private dst=null perm=null proto=rpc
     * 2015-11-19 23:47:29,026 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=open src=/tmp/private dst=null perm=null proto=rpc
     */
    @Test
    public void testRead() throws Exception{
        String e1 = "2015-11-19 23:47:28,922 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=getfileinfo src=/tmp/private dst=null perm=null proto=rpc";
        String e2 = "2015-11-19 23:47:29,026 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=open src=/tmp/private dst=null perm=null proto=rpc";
        String e3 = "2015-11-19 23:47:28,925 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=getfileinfo src=/tmp/private dst=null perm=null proto=rpc";
        String e4 = "2015-11-19 23:47:29,028 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=open src=/tmp/private dst=null perm=null proto=rpc";
        HdfsUserCommandReassembler assembler = new HdfsUserCommandReassembler();
        Config config = ConfigFactory.load();
        assembler.prepareConfig(config);
        assembler.init();

        OutputCollector collector = new OutputCollector(new IOutputCollector() {
            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                return null;
            }

            @Override
            public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {

            }

            @Override
            public void ack(Tuple input) {

            }

            @Override
            public void fail(Tuple input) {

            }

            @Override
            public void resetTimeout(Tuple tuple) {

            }

            @Override
            public void reportError(Throwable error) {

            }
        });
        assembler.flatMap(Arrays.asList("user1", parseEvent(e1)), collector);
        assembler.flatMap(Arrays.asList("user1", parseEvent(e3)), collector);
        assembler.flatMap(Arrays.asList("user1", parseEvent(e2)), collector);
        assembler.flatMap(Arrays.asList("user1", parseEvent(e4)), collector);
        // sleep for a while for Siddhi engine callback to be invoked
        Thread.sleep(100);
    }

    /**
     * 2015-11-20 00:06:47,090 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=getfileinfo src=/tmp/private dst=null perm=null proto=rpc
     * 2015-11-20 00:06:47,185 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=getfileinfo src=/tmp/private._COPYING_ dst=null perm=null proto=rpc
     * 2015-11-20 00:06:47,254 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=create src=/tmp/private._COPYING_ dst=null perm=root:hdfs:rw-r--r-- proto=rpc
     * 2015-11-20 00:06:47,289 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=getfileinfo src=/tmp/private._COPYING_ dst=null perm=null proto=rpc
     * 2015-11-20 00:06:47,609 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=delete src=/tmp/private dst=null perm=null proto=rpc
     * 2015-11-20 00:06:47,624 INFO FSNamesystem.audit: allowed=true ugi=root (auth:SIMPLE) ip=/10.0.2.15 cmd=rename src=/tmp/private._COPYING_ dst=/tmp/private perm=root:hdfs:rw-r--r-- proto=rpc
     */
    @Test
    public void testCopyFromLocal() throws Exception{
        String e1 = "2015-12-04 22:03:11,609 INFO FSNamesystem.audit: allowed=true   ugi=hdfs (auth:SIMPLE)  ip=/10.0.2.15   cmd=getfileinfo src=/tmp/private      dst=null perm=null       proto=rpc";
        String e2 = "2015-12-04 22:03:11,730 INFO FSNamesystem.audit: allowed=true   ugi=hdfs (auth:SIMPLE)  ip=/10.0.2.15   cmd=getfileinfo src=/tmp/private._COPYING_     dst=null        perm=null     proto=rpc";
        String e3 = "2015-12-04 22:03:11,798 INFO FSNamesystem.audit: allowed=true   ugi=hdfs (auth:SIMPLE)  ip=/10.0.2.15   cmd=create      src=/tmp/private._COPYING_     dst=null        perm=hdfs:hdfs:rw-r--r--        proto=rpc";
        String e4 = "2015-12-04 22:03:11,827 INFO FSNamesystem.audit: allowed=true   ugi=hdfs (auth:SIMPLE)  ip=/10.0.2.15   cmd=getfileinfo src=/tmp/private._COPYING_     dst=null        perm=null       proto=rpc";
        String e5 = "2015-12-04 22:03:11,935 INFO FSNamesystem.audit: allowed=true   ugi=hdfs (auth:SIMPLE)  ip=/10.0.2.15   cmd=delete      src=/tmp/private      dst=null perm=null       proto=rpc";
        String e6 = "2015-12-04 22:03:11,945 INFO FSNamesystem.audit: allowed=true   ugi=hdfs (auth:SIMPLE)  ip=/10.0.2.15   cmd=rename      src=/tmp/private._COPYING_     dst=/tmp/private        perm=hdfs:hdfs:rw-r--r--        proto=rpc";
        HdfsUserCommandReassembler assembler = new HdfsUserCommandReassembler();
        Config config = ConfigFactory.load();
        assembler.prepareConfig(config);
        assembler.init();

        OutputCollector collector = new OutputCollector(new IOutputCollector() {
            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                return null;
            }

            @Override
            public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {

            }

            @Override
            public void ack(Tuple input) {

            }

            @Override
            public void fail(Tuple input) {

            }

            @Override
            public void resetTimeout(Tuple tuple) {

            }

            @Override
            public void reportError(Throwable error) {

            }
        });
        assembler.flatMap(Arrays.asList("user1", parseEvent(e1)), collector);
        assembler.flatMap(Arrays.asList("user1", parseEvent(e2)), collector);
        assembler.flatMap(Arrays.asList("user1", parseEvent(e3)), collector);
        assembler.flatMap(Arrays.asList("user1", parseEvent(e4)), collector);
        assembler.flatMap(Arrays.asList("user1", parseEvent(e5)), collector);
        assembler.flatMap(Arrays.asList("user1", parseEvent(e6)), collector);
        // sleep for a while for Siddhi engine callback to be invoked
        Thread.sleep(100);
    }
}
