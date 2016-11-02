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
package org.apache.eagle.topology.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.eagle.topology.entity.HealthCheckParseAPIEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class HealthCheckParseBolt extends BaseRichBolt { 	
	
	private static Logger LOG = LoggerFactory.getLogger(HealthCheckParseBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
    	HealthCheckParseAPIEntity result = null;
        try{
            result = (HealthCheckParseAPIEntity) tuple.getValueByField("f1");
            Map<String, Object> map = new TreeMap<>();
            map.put("status", result.getStatus());
            map.put("timestamp", result.getTimeStamp());
            map.put("role", result.getRole());
            map.put("host", result.getHost());
            map.put("site", result.getSite());

            if(LOG.isDebugEnabled()){
                LOG.debug("emitted " + map);
            }      
            collector.emit(Arrays.asList(result.getHost(), map));
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
