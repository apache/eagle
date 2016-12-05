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

package org.apache.eagle.jpm.util;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    private static final String JOB_SYMBOL = "/jobs";

    public static void closeInputStream(InputStream is) {
        if (is != null) {
            try {
                is.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void sleep(long seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static long dateTimeToLong(String date) {
        // date is like: 2016-07-29T19:35:40.715GMT
        long timestamp = 0L;
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSzzz");
            Date parsedDate = dateFormat.parse(date);
            timestamp = parsedDate.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (timestamp == 0L) {
            LOG.warn("Not able to parse date: " + date);
        }

        return timestamp;
    }

    public static long parseMemory(String memory) {
        if (memory.endsWith("g") || memory.endsWith("G")) {
            int executorGB = Integer.parseInt(memory.substring(0, memory.length() - 1));
            return 1024L * 1024 * 1024 * executorGB;
        } else if (memory.endsWith("m") || memory.endsWith("M")) {
            int executorMB = Integer.parseInt(memory.substring(0, memory.length() - 1));
            return 1024L * 1024 * executorMB;
        } else if (memory.endsWith("k") || memory.endsWith("K")) {
            int executorKB = Integer.parseInt(memory.substring(0, memory.length() - 1));
            return 1024L * executorKB;
        } else if (memory.endsWith("t") || memory.endsWith("T")) {
            int executorTB = Integer.parseInt(memory.substring(0, memory.length() - 1));
            return 1024L * 1024 * 1024 * 1024 * executorTB;
        } else if (memory.endsWith("p") || memory.endsWith("P")) {
            int executorPB = Integer.parseInt(memory.substring(0, memory.length() - 1));
            return 1024L * 1024 * 1024 * 1024 * 1024 * executorPB;
        }
        LOG.warn("Cannot parse memory info " + memory);

        return 0L;
    }

    public static Constants.JobType fetchJobType(Map config) {
        if (config.get(Constants.JobConfiguration.CASCADING_JOB) != null) {
            return Constants.JobType.CASCADING;
        }
        if (config.get(Constants.JobConfiguration.HIVE_JOB) != null) {
            return Constants.JobType.HIVE;
        }
        if (config.get(Constants.JobConfiguration.PIG_JOB) != null) {
            return Constants.JobType.PIG;
        }
        if (config.get(Constants.JobConfiguration.SCOOBI_JOB) != null) {
            return Constants.JobType.SCOOBI;
        }
        return Constants.JobType.NOTAVALIABLE;
    }

    public static Constants.JobType fetchJobType(Configuration config) {
        Map<String, String> mapConfig = new HashMap<>();
        config.forEach(entry -> mapConfig.put(entry.getKey(), entry.getValue()));
        return fetchJobType(mapConfig);
    }

    public static String makeLockPath(String zkrootWithSiteId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(zkrootWithSiteId), "zkrootWithSiteId must not be blank");
        return StringUtils.endsWith(zkrootWithSiteId.toLowerCase(), JOB_SYMBOL)
            ? StringUtils.substring(zkrootWithSiteId.toLowerCase(), 0, StringUtils.lastIndexOf(zkrootWithSiteId.toLowerCase(), JOB_SYMBOL)) + "/locks" :
            zkrootWithSiteId.toLowerCase() + "/locks";
    }
}
