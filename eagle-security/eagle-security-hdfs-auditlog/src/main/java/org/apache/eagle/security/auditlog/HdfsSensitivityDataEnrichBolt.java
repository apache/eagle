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
package org.apache.eagle.security.auditlog;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.typesafe.config.Config;
import org.apache.eagle.security.auditlog.util.SimplifyPath;
import org.apache.eagle.security.service.HdfsSensitivityEntity;
import org.apache.eagle.security.enrich.AbstractDataEnrichBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HdfsSensitivityDataEnrichBolt extends AbstractDataEnrichBolt<HdfsSensitivityEntity, String> {
    private static Logger LOG = LoggerFactory.getLogger(HdfsSensitivityDataEnrichBolt.class);

    public HdfsSensitivityDataEnrichBolt(Config config) {
        super(config, new HdfsSensitivityDataEnrichLCM(config));
    }

    @Override
    public void executeWithEnrich(Tuple input, Map<String, HdfsSensitivityEntity> map) {
        try {
            Map<String, Object> toBeCopied = (Map<String, Object>) input.getValue(0);
            Map<String, Object> event = new TreeMap<String, Object>(toBeCopied);
            HdfsSensitivityEntity e = null;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Receive map: " + map + "event: " + event);
            }

            String src = (String) event.get("src");
            if (map != null && src != null) {
                String simplifiedPath = new SimplifyPath().build(src);
                for (String fileDir : map.keySet()) {
                    Pattern pattern = Pattern.compile(simplifiedPath, Pattern.CASE_INSENSITIVE);
                    Matcher matcher = pattern.matcher(fileDir);
                    boolean isMatched = matcher.matches();
                    if (isMatched) {
                        e = map.get(fileDir);
                        break;
                    }
                }
            }
            event.put("sensitivityType", e == null ? "NA" : e.getSensitivityType());
            if (LOG.isDebugEnabled()) {
                LOG.debug("After file sensitivity lookup: " + event);
            }
            // LOG.info(">>>> After file sensitivity lookup: " + event);
            collector.emit(Arrays.asList(event.get("user"), event));
        } catch (Exception ex) {
            LOG.error("error joining data, ignore it", ex);
        } finally {
            collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user", "message"));
    }
}
