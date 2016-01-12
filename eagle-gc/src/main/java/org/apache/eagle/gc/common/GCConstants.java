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

package org.apache.eagle.gc.common;

import org.apache.commons.lang.time.DateUtils;

public class GCConstants {
    public static String GC_PAUSE_TIME_METRIC_NAME = "eagle.namenode.gc.pausetime";
    public static long GC_PAUSE_TIME_METRIC_GRANULARITY = 2 * DateUtils.MILLIS_PER_MINUTE;
    public static String GC_YOUNG_MEMORY_METRIC_NAME = "eagle.namenode.gc.memory.young.used";
    public static String GC_TENURED_MEMORY_METRIC_NAME = "eagle.namenode.gc.memory.tenured.used";
    public static String GC_TOTAL_MEMORY_METRIC_NAME = "eagle.namenode.gc.memory.total.used";
}
