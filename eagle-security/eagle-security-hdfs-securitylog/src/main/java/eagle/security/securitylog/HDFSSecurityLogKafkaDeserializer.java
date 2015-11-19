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

package eagle.security.securitylog;


import eagle.security.securitylog.parse.HDFSSecurityLogObject;
import org.apache.eagle.dataproc.impl.storm.kafka.SpoutKafkaMessageDeserializer;
import eagle.security.securitylog.parse.HDFSSecurityLogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

public class HDFSSecurityLogKafkaDeserializer implements SpoutKafkaMessageDeserializer {
    Logger LOG = LoggerFactory.getLogger(HDFSSecurityLogKafkaDeserializer.class);

    private Properties props;

    public  HDFSSecurityLogKafkaDeserializer(Properties props){
        this.props = props;
    }

    @Override
    public Object deserialize(byte[] arg0) {
        String logLine = new String(arg0);

        HDFSSecurityLogParser parser = new HDFSSecurityLogParser();
        HDFSSecurityLogObject entity = null;
        try{
            entity = parser.parse(logLine);
        }catch(Exception ex){
            LOG.error("Failing parse audit log message", ex);
        }
        if(entity == null){
            LOG.warn("Event ignored as it can't be correctly parsed, the log is ", logLine);
            return null;
        }
        Map<String, Object> map = new TreeMap<String, Object>();
        map.put("timestamp", entity.timestamp);
        map.put("allowed", entity.allowed);
        map.put("user", entity.user);

        return map;
    }
}
