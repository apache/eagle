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

package org.apache.eagle.jpm.mr.history;

import com.typesafe.config.ConfigFactory;
import org.apache.eagle.jpm.mr.history.crawler.JobHistoryContentFilter;
import org.apache.eagle.jpm.mr.history.crawler.JobHistoryContentFilterBuilder;
import org.apache.eagle.jpm.mr.history.parser.JHFMRVer2EventReader;
import org.apache.eagle.jpm.mr.history.parser.JobConfigurationCreationServiceListener;
import org.apache.eagle.jpm.mr.historyentity.JobBaseAPIEntity;
import org.apache.eagle.jpm.mr.historyentity.JobConfigurationAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class JHFEventReaderBaseTest {

    @Test
    public void testParseConfiguration() throws Exception {
        Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.addResource("job_1479206441898_508949_conf.xml");

        final JobHistoryContentFilterBuilder builder = JobHistoryContentFilterBuilder.newBuilder().acceptJobFile().acceptJobConfFile();
        List<String> confKeyPatterns = new ArrayList<>();
        confKeyPatterns.add(Constants.JobConfiguration.CASCADING_JOB);
        confKeyPatterns.add(Constants.JobConfiguration.HIVE_JOB);
        confKeyPatterns.add(Constants.JobConfiguration.PIG_JOB);
        confKeyPatterns.add(Constants.JobConfiguration.SCOOBI_JOB);
        for (String key : confKeyPatterns) {
            builder.includeJobKeyPatterns(Pattern.compile(key));
        }
        JobHistoryContentFilter filter = builder.build();

        MRHistoryJobConfig appConfig = MRHistoryJobConfig.newInstance(ConfigFactory.load());
        Map<String, String> tags = new HashMap<>();
        tags.put("site", "sandbox");
        tags.put("jobId", "job_1490593856016_152289");
        tags.put("jobType", "HIVE");
        tags.put("jobDefId", "INSERT OVERWRITE TABLE kyl...'2017-04-06')))(Stage-1)");
        JHFMRVer2EventReader reader = new JHFMRVer2EventReader(tags, conf, filter, appConfig);
        reader.addListener(new JobConfigurationCreationServiceListener(appConfig.getEagleServiceConfig()) {
            @Override
            public void jobEntityCreated(JobBaseAPIEntity entity) throws Exception {
                Assert.assertTrue(null != entity);
                Assert.assertTrue(entity instanceof  JobConfigurationAPIEntity);
                JobConfigurationAPIEntity configurationAPIEntity = (JobConfigurationAPIEntity) entity;
                Assert.assertTrue(configurationAPIEntity.getJobConfig().getConfig().size() == 1);
            }
        });
        reader.parseConfiguration();
    }
}
