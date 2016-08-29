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
package org.apache.eagle.security.oozie.parse.sensitivity;

import org.apache.eagle.security.enrich.AbstractDataEnrichBolt;
import org.apache.eagle.security.entity.OozieResourceSensitivityAPIEntity;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

public class OozieResourceSensitivityDataJoinBolt extends AbstractDataEnrichBolt<OozieResourceSensitivityAPIEntity, String> {
    private static final Logger LOG = LoggerFactory.getLogger(OozieResourceSensitivityDataJoinBolt.class);
    private Config config;
    private OutputCollector collector;

    public OozieResourceSensitivityDataJoinBolt(Config config) {
        super(config, new OozieSensitivityDataEnrichLCM(config));
    }

    @Override
    public void executeWithEnrich(Tuple input, Map<String, OozieResourceSensitivityAPIEntity> map) {
        try {
            Map<String, Object> event = (Map<String, Object>) input.getValue(0);
            LOG.info(">>>> event: " + event + " >>>> map: " + map);

            String resource = (String) event.get("jobId");

            OozieResourceSensitivityAPIEntity sensitivityEntity = null;

            if (map != null && !"".equals(resource)) {
                for (String key : map.keySet()) {
                    Pattern pattern = Pattern.compile(key, Pattern.CASE_INSENSITIVE);
                    if (pattern.matcher(resource).find()) {
                        sensitivityEntity = map.get(key);
                        break;
                    }
                }
            }
            Map<String, Object> newEvent = new TreeMap<String, Object>(event);
            newEvent.put("sensitivityType", sensitivityEntity == null ? "NA" : sensitivityEntity.getSensitivityType());
            //newEvent.put("jobId", resource);
            if (LOG.isDebugEnabled()) {
                LOG.debug("After oozie resource sensitivity lookup: " + newEvent);
            }
            LOG.info("After oozie resource sensitivity lookup: " + newEvent);
            collector.emit(Arrays.asList(newEvent.get("user"), newEvent));
        } catch (Exception ex) {
            LOG.error("error join external sensitivity data", ex);
        } finally {
            collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user", "message"));
    }
}
