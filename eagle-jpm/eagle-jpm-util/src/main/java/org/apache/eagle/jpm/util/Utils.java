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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

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
        } catch(ParseException e) {
            e.printStackTrace();
        }

        if (timestamp == 0L) {
            LOG.error("Not able to parse date: " + date);
        }

        return timestamp;
    }

    public static long parseMemory(String memory) {
        if (memory.endsWith("g") || memory.endsWith("G")) {
            int executorGB = Integer.parseInt(memory.substring(0, memory.length() - 1));
            return 1024l * 1024 * 1024 * executorGB;
        } else if (memory.endsWith("m") || memory.endsWith("M")) {
            int executorMB = Integer.parseInt(memory.substring(0, memory.length() - 1));
            return 1024l * 1024 * executorMB;
        } else if (memory.endsWith("k") || memory.endsWith("K")) {
            int executorKB = Integer.parseInt(memory.substring(0, memory.length() - 1));
            return 1024l * executorKB;
        } else if (memory.endsWith("t") || memory.endsWith("T")) {
            int executorTB = Integer.parseInt(memory.substring(0, memory.length() - 1));
            return 1024l * 1024 * 1024 * 1024 * executorTB;
        } else if (memory.endsWith("p") || memory.endsWith("P")) {
            int executorPB = Integer.parseInt(memory.substring(0, memory.length() - 1));
            return 1024l * 1024 * 1024 * 1024 * 1024 * executorPB;
        }
        LOG.info("Cannot parse memory info " +  memory);
        return 0l;
    }
}
