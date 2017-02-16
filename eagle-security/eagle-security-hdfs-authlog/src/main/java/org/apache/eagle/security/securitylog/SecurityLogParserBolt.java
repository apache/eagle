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
package org.apache.eagle.security.securitylog;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

/**
 * Since 6/7/16.
 */
public class SecurityLogParserBolt extends BaseRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(SecurityLogParserBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String logLine = tuple.getString(0);
        HDFSSecurityLogParser parser = new HDFSSecurityLogParser();
        HDFSSecurityLogObject entity = null;
        try{
            entity = parser.parse(logLine);
            Map<String, Object> map = new TreeMap<>();
            map.put("timestamp", entity.timestamp);
            map.put("allowed", entity.allowed);
            map.put("user", entity.user);

            if(LOG.isDebugEnabled()){
                LOG.debug("emitted " + map);
            }

            // push to Kafka sink
            ObjectMapper mapper = new ObjectMapper();
            String msg = mapper.writeValueAsString(map);
            collector.emit(Arrays.asList(entity.user, msg));
        }catch(Exception ex){
            LOG.error("Failing parse security log message, and ignore this message", ex);
        }finally {
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(FieldNameBasedTupleToKafkaMapper.BOLT_KEY, FieldNameBasedTupleToKafkaMapper.BOLT_MESSAGE));
    }
}
