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
package org.apache.eagle.hadoop.metric;

import org.apache.eagle.dataproc.core.JsonSerDeserUtils;
import org.apache.eagle.dataproc.impl.storm.kafka.SpoutKafkaMessageDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Map;

/**
 * Created on 1/19/16.
 */
public class HadoopJmxMetricDeserializer implements SpoutKafkaMessageDeserializer {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopJmxMetricDeserializer.class);

    // convert to a map of <key, map<>>
    @Override
    public Object deserialize(byte[] arg0) {
        try {
            String content = new String(arg0, Charset.defaultCharset().name());
            Map<String, Object> metricItem = JsonSerDeserUtils.deserialize(content, Map.class);
            return metricItem;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("unrecognizable content", e);
        }
        return null;
    }
}
