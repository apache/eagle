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
package org.apache.eagle.security.hive.sensitivity;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import com.typesafe.config.Config;
import org.apache.eagle.security.service.HiveSensitivityEntity;
import org.apache.eagle.security.enrich.AbstractDataEnrichBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveSensitivityDataEnrichBolt extends AbstractDataEnrichBolt<HiveSensitivityEntity, String> {
    private final static Logger LOG = LoggerFactory.getLogger(HiveSensitivityDataEnrichBolt.class);

    public HiveSensitivityDataEnrichBolt(Config config){
        super(config, new HiveSensitivityDataEnrichLCM(config));
    }

    @Override
    public void executeWithEnrich(Tuple input, Map<String, HiveSensitivityEntity> map) {
        String user = input.getString(0);
        Map<String, Object> event = (Map<String, Object>)input.getValue(1);

        String resource = (String)event.get("resource");
        List<String> resourceList = Arrays.asList(resource.split("\\s*,\\s*"));
        HiveSensitivityEntity sensitivityEntity = null;

        // Check if hive resource contains sensitive data.
        for (String s : resourceList) {
            if (map != null) {
                sensitivityEntity = null;
                for (String r : map.keySet()) {
                    Pattern pattern = Pattern.compile(r,Pattern.CASE_INSENSITIVE);
                    Matcher matcher = pattern.matcher(s);
                    boolean isMatched = matcher.matches();
                    if (isMatched) {
                        sensitivityEntity = map.get(r);
                        break;
                    }
                }
            }
            Map<String, Object> newEvent = new TreeMap<String, Object>(event);
            newEvent.put("sensitivityType", sensitivityEntity  == null ?
                    "NA" : sensitivityEntity.getSensitivityType());
            newEvent.put("resource", s);
            if(LOG.isDebugEnabled()) {
                LOG.debug("After hive resource sensitivity lookup: " + newEvent);
            }
            LOG.info("After hive resource sensitivity lookup: " + newEvent);
            collector.emit(Arrays.asList(user, newEvent));
            collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user", "message"));
    }
}
