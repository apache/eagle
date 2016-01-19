/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.hadoop.metric;

import junit.framework.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Created on 1/19/16.
 */
public class HadoopJmxMetricDeserializerTest {

    @Test
    public void test() {
        HadoopJmxMetricDeserializer des = new HadoopJmxMetricDeserializer();

        String m = "{\"host\": \"hostname-1\", \"timestamp\": 1453208956395, \"metric\": \"hadoop.namenode.dfs.lastwrittentransactionid\", \"component\": \"namenode\", \"site\": \"sandbox\", \"value\": \"49716\"}";
        Object obj = des.deserialize(m.getBytes());
        Assert.assertTrue(obj instanceof Map);
        Map<String, Object> metric = (Map<String, Object>) obj;
        Assert.assertEquals("hostname-1" ,metric.get("host"));
        Assert.assertEquals(1453208956395l ,metric.get("timestamp"));
    }
}
