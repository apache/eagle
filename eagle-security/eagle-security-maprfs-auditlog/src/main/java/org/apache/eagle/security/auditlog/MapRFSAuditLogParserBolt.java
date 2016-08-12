/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  * <p/>
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  * <p/>
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.security.auditlog;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.eagle.security.hdfs.MAPRFSAuditLogObject;
import org.apache.eagle.security.hdfs.MAPRFSAuditLogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

/**
 * Since 8/11/16.
 */
public class MapRFSAuditLogParserBolt extends BaseRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(HdfsAuditLogParserBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String logLine = input.getString(0);

        MAPRFSAuditLogParser parser = new MAPRFSAuditLogParser();
        MAPRFSAuditLogObject entity = null;
        try{
            entity = parser.parse(logLine);
            Map<String, Object> map = new TreeMap<String, Object>();
            map.put("src", entity.src);
            map.put("dst", entity.dst);
            map.put("host", entity.host);
            map.put("timestamp", entity.timestamp);
            map.put("status", entity.status);
            map.put("user", entity.user);
            map.put("cmd", entity.cmd);
            map.put("volume", entity.volume);
            collector.emit(Arrays.asList(map));
        }catch(Exception ex) {
            LOG.error("Failing parse audit log message", ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("f1"));
    }
}
