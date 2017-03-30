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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Since 6/7/16.
 */
public class HBaseAuditLogParserBolt extends BaseRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(HBaseAuditLogParserBolt.class);
    private OutputCollector collector;
    private static final HBaseAuditLogParser parser = new HBaseAuditLogParser();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String logLine = input.getString(0);
        try {
            HBaseAuditLogObject entity = parser.parse(logLine);
            Map<String, Object> map = new TreeMap<>();
            map.put("action", entity.action);
            map.put("host", entity.host);
            map.put("status", entity.status);
            map.put("request", entity.request);
            map.put("scope", entity.scope);
            map.put("user", entity.user);
            map.put("timestamp", entity.timestamp);
            collector.emit(Collections.singletonList(map));
        } catch (Exception ex) {
            LOG.error("Failing parse and ignore audit log {} ", logLine, ex);
        } finally {
            collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("f1"));
    }
}
