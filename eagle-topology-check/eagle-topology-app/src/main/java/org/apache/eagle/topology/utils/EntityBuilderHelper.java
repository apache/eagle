/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.topology.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.topology.TopologyConstants;
import org.apache.eagle.topology.entity.TopologyBaseAPIEntity;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class EntityBuilderHelper {

    public static String resolveHostByIp(String ip) {
        InetAddress addr = null;
        try {
            addr = InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return addr.getHostName();
    }

    private static GenericMetricEntity metricWrapper(Long timestamp, String metricName, double value, Map<String, String> tags) {
        GenericMetricEntity metricEntity = new GenericMetricEntity();
        metricEntity.setTimestamp(timestamp);
        metricEntity.setTags(tags);
        metricEntity.setPrefix(metricName);
        metricEntity.setValue(new double[] {value});
        return metricEntity;
    }

    public static GenericMetricEntity generateMetric(String role, double value, String site, long timestamp) {
        Map<String, String> tags = new HashMap<>();
        tags.put(TopologyConstants.SITE_TAG, site);
        tags.put(TopologyConstants.ROLE_TAG, role);
        String metricName = String.format(TopologyConstants.METRIC_LIVE_RATIO_NAME_FORMAT, role);
        return EntityBuilderHelper.metricWrapper(timestamp, metricName, value, tags);
    }

    public static String getValidHostName(String key) {
        if (StringUtils.isBlank(key)) {
            throw new IllegalArgumentException("key can not be empty");
        }
        return key.indexOf(TopologyConstants.COLON) > 0 ? key.substring(0, key.indexOf(TopologyConstants.COLON)) : key;
    }

    public static String generateKey(TopologyBaseAPIEntity entity) {
        return new HashCodeBuilder().append(entity.getTags().get(TopologyConstants.SITE_TAG))
                .append(entity.getTags().get(TopologyConstants.HOSTNAME_TAG))
                .append(entity.getTags().get(TopologyConstants.ROLE_TAG))
                .build().toString();
    }

}
