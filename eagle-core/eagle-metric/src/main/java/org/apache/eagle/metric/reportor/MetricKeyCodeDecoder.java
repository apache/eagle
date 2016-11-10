/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package org.apache.eagle.metric.reportor;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO: Refactor metric implementation, which is not very solid.
 *
 * <p>metricKey = metricName[/[key=char|whitespace]*]*
 */
@Deprecated
public class MetricKeyCodeDecoder {
    private static final String SEPERATOR = "/";

    public static String codeMetricKey(String metricName, Map<String, String> tags) {
        StringBuilder sb = new StringBuilder();
        sb.append(metricName);
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.contains(SEPERATOR) || value.contains(SEPERATOR)) {
                throw new IllegalStateException("Invalid metric tag pair <" + key + ":" + value + "> which contains reserved char: " + SEPERATOR);
            }
            sb.append(SEPERATOR).append(key).append(":").append(value);
        }
        return sb.toString();
    }

    /**
     * @param name metricName[/[key=char|whitespace]*]*.
     * @return
     */
    public static EagleMetricKey decodeMetricKey(String name) {
        EagleMetricKey metricName = new EagleMetricKey();
        String[] parts = name.split(SEPERATOR);
        metricName.metricName = parts[0];
        metricName.tags = new HashMap<>();
        for (int i = 1; i < parts.length; i++) {
            String[] keyValue = parts[i].split(":");
            if (keyValue.length > 1) {
                metricName.tags.put(keyValue[0], keyValue[1]);
            } else {
                throw new IllegalStateException("Failed to decode metric name '" + name + "', because '" + parts[i] + "' is invalid, expected in format: 'key:value'");
            }
        }
        return metricName;
    }

    public static String addTimestampToMetricKey(long timestamp, String metricKey) {
        return timestamp + SEPERATOR + metricKey;
    }

    public static String codeTSMetricKey(long timestamp, String metricName, Map<String, String> tags) {
        return addTimestampToMetricKey(timestamp, codeMetricKey(metricName, tags));
    }

    public static EagleMetricKey decodeTSMetricKey(String name) {
        Integer index = name.indexOf(SEPERATOR);
        EagleMetricKey metricKey = decodeMetricKey(name.substring(index + 1));
        metricKey.timestamp = Long.valueOf(name.substring(0, index));
        return metricKey;
    }
}
