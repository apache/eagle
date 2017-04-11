/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.jpm.mr;

import org.apache.eagle.jpm.mr.historyentity.JobConfig;
import org.apache.eagle.jpm.mr.historyentity.JobConfigSerDeser;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class JobConfigSerDeserTest {

    @Test
    public void test() {
        String value = "INSERT OVERWRITE TABLE kylin_intermediate_KYLIN_HIVE_METRICS_QUERY_CUBE SELECT\n" +
                "HIVE_METRICS_QUERY_CUBE.`CUBE_NAME`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`SEGMENT_NAME`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`CUBOID_SOURCE`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`CUBOID_TARGET`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`IF_MATCH`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`IF_SUCCESS`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`KYEAR_BEGIN_DATE`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`KMONTH_BEGIN_DATE`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`KWEEK_BEGIN_DATE`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`KDAY_DATE`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`WEIGHT_PER_HIT`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`STORAGE_CALL_COUNT`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`STORAGE_CALL_TIME_SUM`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`STORAGE_CALL_TIME_MAX`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`STORAGE_COUNT_SKIP`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`STORAGE_SIZE_SCAN`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`STORAGE_SIZE_RETURN`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`STORAGE_SIZE_AGGREGATE_FILTER`\n" +
                ",HIVE_METRICS_QUERY_CUBE.`STORAGE_SIZE_AGGREGATE`\n" +
                "FROM KYLIN.HIVE_METRICS_QUERY_CUBE as HIVE_METRICS_QUERY_CUBE\n" +
                "WHERE (((HIVE_METRICS_QUERY_CUBE.KDAY_DATE = '2017-04-06' AND HIVE_METRICS_QUERY_CUBE.KDAY_TIME >= '18:00:00') OR (HIVE_METRICS_QUERY_CUBE.KDAY_DATE > '2017-04-06')) AND ((HIVE_METRICS_QUERY_CUBE.KDAY_DATE = '2017-04-06' AND HIVE_METRICS_QUERY_CUBE.KDAY_TIME < '20:00:00') OR (HIVE_METRICS_QUERY_CUBE.KDAY_DATE < '2017-04-06')))";

        Map<String, String> conf = new HashMap<>();
        conf.put("test1", value);
        JobConfig source = new JobConfig();
        source.setConfig(conf);
        JobConfigSerDeser serDeser = new JobConfigSerDeser();
        byte[] bytes = serDeser.serialize(source);
        JobConfig target = serDeser.deserialize(bytes);

        Assert.assertTrue(target.getConfig().size() == 1);
    }
}
