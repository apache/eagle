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
import com.typesafe.config.Config;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.security.hdfs.HDFSAuditLogObject;
import org.apache.eagle.security.hdfs.HDFSAuditLogParser;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * Since 8/10/16.
 */
public class HdfsAuditLogParserBolt extends BaseRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(HdfsAuditLogParserBolt.class);
    private static final String DATASOURCE_TIMEZONE_PATH = "dataSourceConfig.timeZone";

    private OutputCollector collector;
    private HDFSAuditLogParser parser;

    public HdfsAuditLogParserBolt(Config config) {
        if (config.hasPath(DATASOURCE_TIMEZONE_PATH)) {
            TimeZone timeZone = TimeZone.getTimeZone(config.getString(DATASOURCE_TIMEZONE_PATH));
            parser = new HDFSAuditLogParser(timeZone);
        } else {
            parser = new HDFSAuditLogParser();
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String logLine = input.getString(0);
        HDFSAuditLogObject entity = null;
        try {
            entity = parser.parse(logLine);
            Map<String, Object> map = new TreeMap<>();
            map.put(HDFSAuditLogObject.HDFS_SRC_KEY, entity.src);
            map.put(HDFSAuditLogObject.HDFS_DST_KEY, entity.dst);
            map.put(HDFSAuditLogObject.HDFS_HOST_KEY, entity.host);
            map.put(HDFSAuditLogObject.HDFS_TIMESTAMP_KEY, entity.timestamp);
            map.put(HDFSAuditLogObject.HDFS_ALLOWED_KEY, entity.allowed);
            map.put(HDFSAuditLogObject.HDFS_USER_KEY, entity.user);
            map.put(HDFSAuditLogObject.HDFS_CMD_KEY, entity.cmd);
            collector.emit(input, Collections.singletonList(map));
        } catch (Exception ex) {
            LOG.error("Failing parse audit log message {}", logLine, ex);
        } finally {
            collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("f1"));
    }
}
