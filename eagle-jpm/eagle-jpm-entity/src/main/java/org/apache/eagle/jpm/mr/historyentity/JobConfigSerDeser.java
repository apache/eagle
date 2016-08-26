/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.mr.historyentity;

import org.apache.eagle.log.entity.meta.EntitySerDeser;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class JobConfigSerDeser implements EntitySerDeser<JobConfig> {

    @Override
    public JobConfig deserialize(byte[] bytes) {
        JobConfig jc = new JobConfig();
        Map<String, String> map = new TreeMap<String, String>();
        jc.setConfig(map);
        String sb = Bytes.toString(bytes);
        String[] keyValue = sb.split(",");
        for (String pair : keyValue) {
            String[] str = pair.split(":");
            if (pair.equals("") || str[0].equals("")) {
                continue;
            }
            String key = str[0];
            String value = "";
            if (str.length == 2) {
                value = str[1];
            }
            map.put(key, value);
        }
        return jc;
    }
    
    @Override
    public byte[] serialize(JobConfig conf) {
        Map<String, String> map = conf.getConfig();
        StringBuilder sb = new StringBuilder();
        for (Entry<String, String> entry : map.entrySet()) {
            sb.append(entry.getKey() + ":" + entry.getValue() + ",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString().getBytes();
    }

    @Override
    public Class<JobConfig> type() {
        return JobConfig.class;
    }
}
