/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.eagle.jobrunning.job.conf;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.Map;

public class TestJobConfParserImpl {

    public static final String SQL = "CREATE TABLE XXX.XXX as SELECT /*+ MAPJOIN(XXX,XXX) */ trim(x.XXX) AS hc.XXX, hc.XXX, SUM(x.XXX) AS XXX FROM XXX.XXX x INNER JOIN XXX.XXX XXX ON x.XXX = XXX.XXX AND XXX.XXX = 1 INNER JOIN XXX.XXX dp ON XXX.XXX = XXX.XXX AND XXX.XXX = 1 INNER JOIN XXX.XXX hc ON XXX.XXX = XXX.XXX AND XXX.XXX=1 LEFT OUTER JOIN XXX.XXX hsc ON hsc.XXX = hc.XXX AND hsc.XXX=1 WHERE x.ds = 'XXX' AND length(x.XXX) > 0 AND x.XXX = 51 GROUP BY trim(x.XXX), hc.XXX, hc.XXX";

    @Test
    public void test() throws Exception {
        InputStream is = this.getClass().getResourceAsStream("/jobconf.html");
        final Document doc = Jsoup.parse(is, "UTF-8", "{historyUrl}/jobhistory/conf/job_xxxxxxxxxxxxx_xxxxxx");
        JobConfParser parser = new JobConfParserImpl();
        Map<String, String> configs = parser.parse(doc);
        Assert.assertEquals(configs.size(), 3);
        Assert.assertEquals(SQL, configs.get("mapreduce.workflow.name"));
        Assert.assertEquals("0.0.0.0:50070", configs.get("dfs.namenode.http-address"));
        Assert.assertEquals("org.apache.hive.hcatalog.api.repl.exim.EximReplicationTaskFactory", configs.get("hive.repl.task.factory"));
    }
}
