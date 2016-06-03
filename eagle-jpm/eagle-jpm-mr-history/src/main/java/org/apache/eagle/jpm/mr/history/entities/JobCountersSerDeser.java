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

package org.apache.eagle.jpm.mr.history.entities;

import org.apache.eagle.jpm.mr.history.jobcounter.*;
import org.apache.eagle.log.entity.meta.EntitySerDeser;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class JobCountersSerDeser implements EntitySerDeser<JobCounters> {

    private CounterGroupDictionary dictionary = null;

    @Override
    public JobCounters deserialize(byte[] bytes) {
        JobCounters counters = new JobCounters();
        final int length = bytes.length;
        if (length < 4) {
            return counters;
        }

        final Map<String, Map<String, Long> > groupMap = counters.getCounters();
        int pos = 0;
        final int totalGroups = Bytes.toInt(bytes, pos);
        pos += 4;
        
        for (int i = 0; i < totalGroups; ++i) {
            final int groupIndex = Bytes.toInt(bytes, pos);
            pos += 4;
            final int totalCounters = Bytes.toInt(bytes, pos);
            pos += 4;
            final int nextGroupPos = pos + (totalCounters * 12);
            try {
                final CounterGroupKey groupKey = getCounterGroup(groupIndex);
                if (groupKey == null) {
                    throw new JobCounterException("Group index " + groupIndex + " is not defined");
                }
                final Map<String, Long> counterMap = new TreeMap<String, Long>();
                groupMap.put(groupKey.getName(), counterMap);
                for (int j = 0; j < totalCounters; ++j) {
                    final int counterIndex = Bytes.toInt(bytes, pos);
                    pos += 4;
                    final long value = Bytes.toLong(bytes, pos);
                    pos += 8;
                    final CounterKey counterKey = groupKey.getCounterKeyByID(counterIndex);
                    if (counterKey == null) {
                        continue;
                    }
                    counterMap.put(counterKey.getNames().get(0), value);
                }
            } catch (JobCounterException ex) {
                // skip the group
                pos = nextGroupPos;
            }
        }
        return counters;
    }

    @Override
    public byte[] serialize(JobCounters counters) {
        
        final Map<String, Map<String, Long>> groupMap = counters.getCounters();
        int totalSize = 4;
        for (Map<String, Long> counterMap : groupMap.values()) {
            final int counterCount = counterMap.size();
            totalSize += counterCount * 12 + 8;
        }
        byte[] buffer = new byte[totalSize];

        int totalGroups = 0;
        int pos = 0;
        int totalGroupNumberPos = pos;
        pos += 4;
        int nextGroupPos = pos;
        
        for (Map.Entry<String, Map<String, Long>> entry : groupMap.entrySet()) {
            final String groupName = entry.getKey();
            final Map<String, Long> counterMap = entry.getValue();
            try {
                nextGroupPos = pos = serializeGroup(buffer, pos, groupName, counterMap);
                ++totalGroups;
            } catch (JobCounterException ex) {
                pos = nextGroupPos;
            }
        }
        
        Bytes.putInt(buffer, totalGroupNumberPos, totalGroups);
        if (pos < totalSize) {
            buffer = Arrays.copyOf(buffer, pos);
        }
        return buffer;
    }

    @Override
    public Class<JobCounters> type() {
        return JobCounters.class;
    }

    private int serializeGroup(byte[] buffer, int currentPos, String groupName, Map<String, Long> counterMap) throws JobCounterException {
        int pos = currentPos;
        final CounterGroupKey groupKey = getCounterGroup(groupName);
        if (groupKey == null) {
            throw new JobCounterException("Group name " + groupName + " is not defined");
        }
        Bytes.putInt(buffer, pos, groupKey.getIndex());
        pos += 4;
        int totalCounterNumberPos = pos;
        pos += 4;
        int totalCounters = 0;
        
        for (Map.Entry<String, Long> entry : counterMap.entrySet()) {
            final String counterName = entry.getKey();
            final CounterKey counterKey = groupKey.getCounterKeyByName(counterName);
            if (counterKey == null) {
                continue;
            }
            final Long counterValue = entry.getValue();
            Bytes.putInt(buffer, pos, counterKey.getIndex());
            pos += 4;
            Bytes.putLong(buffer, pos, counterValue);
            pos += 8;
            ++totalCounters;
        }
        Bytes.putInt(buffer, totalCounterNumberPos, totalCounters);
        return pos;
    }

    private CounterGroupKey getCounterGroup(String groupName) throws JobCounterException {
        if (dictionary == null) {
            dictionary = CounterGroupDictionary.getInstance();
        }
        final CounterGroupKey groupKey = dictionary.getCounterGroupByName(groupName);
        if (groupKey == null) {
            throw new JobCounterException("Invalid counter group name: " + groupName);
        }
        return groupKey;
    }

    private CounterGroupKey getCounterGroup(int groupIndex) throws JobCounterException {
        if (dictionary == null) {
            dictionary = CounterGroupDictionary.getInstance();
        }
        return dictionary.getCounterGroupByIndex(groupIndex);
    }


}
