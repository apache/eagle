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
package org.apache.eagle.security.oozie.parse;

import org.apache.eagle.dataproc.impl.storm.kafka.SpoutKafkaMessageDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


public class OozieAuditLogKafkaDeserializer implements SpoutKafkaMessageDeserializer {
    private static Logger LOG = LoggerFactory.getLogger(OozieAuditLogKafkaDeserializer.class);

    public OozieAuditLogKafkaDeserializer() {
    }

    @Override
    public Object deserialize(byte[] arg0) {
        String logLine = new String(arg0);

        OozieAuditLogParser parser = new OozieAuditLogParser();
        OozieAuditLogObject entity = null;
        try {
            entity = parser.parse(logLine);
        } catch (Exception ex) {
            LOG.error("Failing oozie parse audit log message", ex);
        }
        if (entity == null) {
            LOG.warn("Event ignored as it can't be correctly parsed, the log is ", logLine);
            return new HashMap<>();
        }
        Map<String, Object> map = new TreeMap<String, Object>();
        map.put("timestamp", entity.timestamp);
        map.put("level", entity.level);
        map.put("ip", entity.ip);
        map.put("user", entity.user);
        map.put("group", entity.group);
        map.put("app", entity.app);
        map.put("jobId", entity.jobId);
        map.put("operation", entity.operation);
        map.put("parameter", entity.parameter);
        map.put("status", entity.status);
        map.put("httpcode", entity.httpcode);
        map.put("errorcode", entity.errorcode);
        map.put("errormessage", entity.errormessage);

        return map;
    }

}
