/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.mr.history;

import com.dumbster.smtp.SimpleSmtpServer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.common.DateTimeUtil;
import org.junit.After;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.eagle.jpm.mr.history.MRHistoryJobDailyReporter.*;

public class MRHistoryJobDailyReporterTest {

    private static final Logger LOG = LoggerFactory.getLogger(MRHistoryJobDailyReporterTest.class);
    private static final int SMTP_PORT = 5025;
    private Config config;
    private SimpleSmtpServer server;

    @Before
    public void setUp() {
        config = ConfigFactory.load("application-test.conf");
        server = SimpleSmtpServer.start(SMTP_PORT);
    }

    @After
    public void clear() {
        if ( server != null ) {
            server.stop();
        }
    }

    @Test
    public void test() throws Exception {
        MRHistoryJobDailyReporter reporter = new MRHistoryJobDailyReporter(config);
        reporter.sendByEmail(mockAlertData());
        Iterator it = server.getReceivedEmail();
        Assert.assertTrue(server.getReceivedEmailSize() == 1);
        while (it.hasNext()) {
            LOG.info(it.next().toString());
        }
    }

    private Map<String, Object> mockAlertData() {
        Map<String, Object> alertData = new HashMap<>();
        List<JobSummaryInfo> summeryInfos = new ArrayList<>();
        summeryInfos.add(buildJobSummaryInfo("FAILED", 8, 8));
        summeryInfos.add(buildJobSummaryInfo("SUCCEEDED", 90, 89.9));
        summeryInfos.add(buildJobSummaryInfo("KILLED", 2, 2));
        alertData.put(SUMMARY_INFO_KEY, summeryInfos);

        List<JobSummaryInfo> failedJobUsers = new ArrayList<>();
        failedJobUsers.add(buildJobSummaryInfo("alice", 100L, 98d));
        failedJobUsers.add(buildJobSummaryInfo("bob", 97L, 2));
        alertData.put(FAILED_JOB_USERS_KEY, failedJobUsers);

        List<JobSummaryInfo> succeededJobUsers = new ArrayList<>();
        succeededJobUsers.add(buildJobSummaryInfo("alice1", 100L, 98));
        succeededJobUsers.add(buildJobSummaryInfo("bob1", 97, 2));
        alertData.put(SUCCEEDED_JOB_USERS_KEY, succeededJobUsers);


        List<JobSummaryInfo> finishedJobUsers = new ArrayList<>();
        finishedJobUsers.add(buildJobSummaryInfo("alice2", 100L, 98));
        finishedJobUsers.add(buildJobSummaryInfo("bob2", 97, 2));
        alertData.put(FINISHED_JOB_USERS_KEY, finishedJobUsers);

        alertData.put(ALERT_TITLE_KEY, "[TEST_CLUSTER] Daily Job Report");
        alertData.put(NUM_TOP_USERS_KEY, 2);
        alertData.put(JOB_OVERTIME_LIMIT_KEY, 6);
        long currentTimestamp = System.currentTimeMillis();
        alertData.put(REPORT_RANGE_KEY, String.format(" %s ~ %s %s",
            DateTimeUtil.millisecondsToHumanDateWithSeconds(currentTimestamp - 6 * DateTimeUtil.ONEHOUR),
            DateTimeUtil.millisecondsToHumanDateWithSeconds(currentTimestamp), DateTimeUtil.CURRENT_TIME_ZONE.getID()));
        alertData.put(EAGLE_JOB_LINK_KEY, "http://localhost:9090/#/site/sandbox/jpm/statistics");
        return alertData;
    }

    private JobSummaryInfo buildJobSummaryInfo(String key, long number, double ratio) {
        JobSummaryInfo jobSummaryInfo = new JobSummaryInfo();
        jobSummaryInfo.numOfJobs = number;
        jobSummaryInfo.ratio = ratio;
        jobSummaryInfo.key = key;
        return jobSummaryInfo;
    }

}
