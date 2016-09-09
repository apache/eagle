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

package org.apache.eagle.service.jpm;

import org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.generic.GenericEntityServiceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ResourceUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceUtils.class);

    static GenericEntityServiceResource resource = new GenericEntityServiceResource();

    public static GenericServiceAPIResponseEntity getQueryResult(String query, String startTime, String endTime) {
        return resource.search(query, startTime, endTime, Integer.MAX_VALUE, null, false, true, 0L, 0, true, 0, null, false);
    }

    public static double[] getCounterValues(List<TaskExecutionAPIEntity> tasks, JobCounters.CounterName counterName) {
        List<Double> values = new ArrayList<>();
        for (TaskExecutionAPIEntity task : tasks) {
            values.add(Double.valueOf(task.getJobCounters().getCounterValue(counterName)));
        }
        return toArray(values);
    }

    public static double[] toArray(List<Double> input) {
        double[] result = new double[input.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = input.get(i);
        }
        return result;
    }

    public static List<Long> parseDistributionList(String timelist) {
        List<Long> times = new ArrayList<>();
        String [] strs = timelist.split("[,\\s]");
        for (String str : strs) {
            try {
                times.add(Long.parseLong(str));
            } catch (Exception ex) {
                LOG.warn(str + " is not a number");
            }
        }
        return times;
    }

    public static int getDistributionPosition(List<Long> rangeList, Long value) {
        for (int i = 1; i < rangeList.size(); i++) {
            if (value < rangeList.get(i)) {
                return i - 1;
            }
        }
        return rangeList.size() - 1;
    }

    public static boolean isDuplicate(Set<String> keys, String jobId) {
        if (keys.isEmpty()) {
            return false;
        }
        return keys.contains(jobId);
    }

}
