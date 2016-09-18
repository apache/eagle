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

import org.apache.eagle.log.entity.GenericMetricEntity;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public class EntityBuilderHelper {

    public static String resolveRackByHost(String hostname) {
        String result = null;
        try {
            InetAddress address = InetAddress.getByName(hostname);
            result = "rack" + (int)(address.getAddress()[2] & 0xff);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String resolveHostByIp(String ip) {
        InetAddress addr = null;
        try {
            addr = InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return addr.getHostName();
    }

    public static GenericMetricEntity metricWrapper(Long timestamp, String metricName, double value, Map<String, String> tags) {
        GenericMetricEntity metricEntity = new GenericMetricEntity();
        metricEntity.setTimestamp(timestamp);
        metricEntity.setTags(tags);
        metricEntity.setPrefix(metricName);
        metricEntity.setValue(new double[]{value});
        return metricEntity;
    }

}
