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

public class MRHistoryJobDailyReporterTest {

    private static final Logger LOG = LoggerFactory.getLogger(MRHistoryJobDailyReporterTest.class);
    private static final int SMTP_PORT = 5025;
    private Config config;
    private SimpleSmtpServer server;

    @Before
    public void setUp(){
        config = ConfigFactory.load("application-test.conf");
        server = SimpleSmtpServer.start(SMTP_PORT);
    }

    @After
    public void clear(){
        if(server!=null) {
            server.stop();
        }
    }
    @Test
    public void test() throws Exception {
        MRHistoryJobDailyReporter reporter = new MRHistoryJobDailyReporter(config, null);
        reporter.sendByEmail(mockAlertData());
        Iterator it = server.getReceivedEmail();
        Assert.assertTrue(server.getReceivedEmailSize() == 1);
        while (it.hasNext()) {
            LOG.info(it.next().toString());
        }
    }

    private Map<String, Object> mockAlertData() {
        Map<String, Object> alertData = new HashMap<>();
        List<MRHistoryJobDailyReporter.JobSummeryInfo> summeryInfos = new ArrayList<>();
        MRHistoryJobDailyReporter.JobSummeryInfo summeryInfo1 = new MRHistoryJobDailyReporter.JobSummeryInfo();
        summeryInfo1.status = "failed";
        summeryInfo1.numOfJobs = 10;
        summeryInfo1.ratio = "0.1";
        MRHistoryJobDailyReporter.JobSummeryInfo summeryInfo2 = new MRHistoryJobDailyReporter.JobSummeryInfo();
        summeryInfo2.status = "succeeded";
        summeryInfo2.numOfJobs = 90;
        summeryInfo2.ratio = "0.9";
        summeryInfos.add(summeryInfo1);
        summeryInfos.add(summeryInfo2);
        alertData.put("summeryInfo", summeryInfos);

        Map<String,Double> failedJobUsers = new TreeMap<>();
        failedJobUsers.put("alice", 100d);
        failedJobUsers.put("bob", 97d);
        alertData.put("failedJobUsers", failedJobUsers);


        Map<String,Double> succeededJobUsers = new TreeMap<>();
        succeededJobUsers.put("alice1", 100d);
        succeededJobUsers.put("bob1", 97d);
        alertData.put("succeededJobUsers", succeededJobUsers);


        Map<String,Double> finishedJobUsers = new TreeMap<>();
        finishedJobUsers.put("alice2", 100d);
        finishedJobUsers.put("bob2", 97d);
        alertData.put("finishedJobUsers", finishedJobUsers);

        alertData.put("alertTitle", "Daily Job Report");
        alertData.put("numTopUsers", 2);
        alertData.put("jobOvertimeLimit", 6);
        long currentTimestamp = System.currentTimeMillis();
        alertData.put("reportRange", String.format(" %s ~ %s %s",
            DateTimeUtil.millisecondsToHumanDateWithSeconds(currentTimestamp - 6 * DateTimeUtil.ONEHOUR),
            DateTimeUtil.millisecondsToHumanDateWithSeconds(currentTimestamp), DateTimeUtil.CURRENT_TIME_ZONE.getID()));
        alertData.put("joblink", "http://localhost:9090");
        return alertData;
    }

}
