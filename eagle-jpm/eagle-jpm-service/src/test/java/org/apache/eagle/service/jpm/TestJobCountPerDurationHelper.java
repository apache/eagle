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

import org.apache.eagle.common.DateTimeUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class TestJobCountPerDurationHelper {
    JobCountPerDurationHelper helper = new JobCountPerDurationHelper();

    private static final Logger LOG = LoggerFactory.getLogger(TestJobCountPerDurationHelper.class);
    @Test
    public void test() throws ParseException {
        String timeString = "2016-08-22 20:13:00";
        long timestamp = DateTimeUtil.humanDateToSeconds(timeString);
        String timeString2 = DateTimeUtil.secondsToHumanDate(timestamp);
        Assert.assertTrue(timeString2.equals(timeString));

        String timeString3 = helper.moveTimeforwardOneDay(timeString);
        Assert.assertTrue(timeString3.equals("2016-08-21 20:13:00"));
    }

    @Test
    public void test2() throws ParseException {
        String startTime = "2016-08-22 20:13:00";
        String endTime = "2016-08-22 24:13:00";
        List<MRJobTaskCountResponse.UnitJobCount> jobCounts = new ArrayList<>();
        helper.initJobCountList(jobCounts, DateTimeUtil.humanDateToSeconds(startTime), DateTimeUtil.humanDateToSeconds(endTime), 15 * 60);
        /*for (MRJobTaskCountResponse.UnitJobCount jobCount : jobCounts) {
            LOG.info(DateTimeUtil.secondsToHumanDate(jobCount.timeBucket));
        }*/
        Assert.assertTrue(DateTimeUtil.secondsToHumanDate(jobCounts.get(1).timeBucket).equals("2016-08-22 20:15:00"));
    }

    @Test
    public void test3() {
        List<MRJobTaskCountResponse.UnitJobCount> jobCounts = new ArrayList<>();
        long intervalSecs = 5;
        helper.initJobCountList(jobCounts, 3, 31, intervalSecs);
        helper.countJob(jobCounts, 5, 10, intervalSecs);
        helper.countJob(jobCounts, 13, 18, intervalSecs);
        helper.countJob(jobCounts, 18, 28, intervalSecs);
        helper.countJob(jobCounts, 25, 33, intervalSecs);
        Assert.assertTrue(jobCounts.size() == 7);
        Assert.assertTrue(jobCounts.get(1).jobCount == 1);
        Assert.assertTrue(jobCounts.get(5).jobCount == 2);
    }

    @Test
    public void test4() throws ParseException {
        List<MRJobTaskCountResponse.UnitJobCount> jobCounts = new ArrayList<>();
        long intervalSecs = 60*15;
        String startTime = "2016-08-22 20:13:00";
        String endTime = "2016-08-22 24:13:00";
        helper.initJobCountList(jobCounts, DateTimeUtil.humanDateToSeconds(startTime), DateTimeUtil.humanDateToSeconds(endTime), intervalSecs);
        helper.countJob(jobCounts,
                DateTimeUtil.humanDateToSeconds("2016-08-22 20:23:00"),
                DateTimeUtil.humanDateToSeconds("2016-08-22 20:30:00"),
                intervalSecs);
        Assert.assertTrue(jobCounts.get(2).jobCount == 1);
    }
}
