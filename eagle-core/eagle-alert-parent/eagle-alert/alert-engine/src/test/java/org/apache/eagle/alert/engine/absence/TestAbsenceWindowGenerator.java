/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.absence;

import org.apache.eagle.alert.engine.evaluator.absence.AbsenceDailyRule;
import org.apache.eagle.alert.engine.evaluator.absence.AbsenceWindow;
import org.apache.eagle.alert.engine.evaluator.absence.AbsenceWindowGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Since 7/8/16.
 */
public class TestAbsenceWindowGenerator {
    @Test
    public void testWindowInToday() throws Exception{
        AbsenceDailyRule rule = new AbsenceDailyRule();
        // from 2PM to 3PM each day
        rule.startOffset = 14*3600*1000;
        rule.endOffset = 15*3600*1000;
        AbsenceWindowGenerator generator = new AbsenceWindowGenerator(rule);

        // get current time
        String date = "2016-07-08 00:00:00";
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date d = df.parse(date);
        long startTimeOfDay = d.getTime();

        String currDate = "2016-07-08 11:30:29";
        df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        d = df.parse(currDate);
        AbsenceWindow window = generator.nextWindow(d.getTime());
        Assert.assertEquals(startTimeOfDay+rule.startOffset, window.startTime);
    }

    @Test
    public void testWindowInTomorrow() throws Exception{
        AbsenceDailyRule rule = new AbsenceDailyRule();
        // from 2PM to 3PM each day
        rule.startOffset = 14*3600*1000;
        rule.endOffset = 15*3600*1000;
        AbsenceWindowGenerator generator = new AbsenceWindowGenerator(rule);

        // get current time
        String date = "2016-07-08 00:00:00";
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date d = df.parse(date);
        long startTimeOfDay = d.getTime();

        String currDate = "2016-07-08 18:20:19";
        df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        d = df.parse(currDate);
        AbsenceWindow window = generator.nextWindow(d.getTime());
        // this needs adjustment for one day
        Assert.assertEquals(startTimeOfDay+rule.startOffset + AbsenceDailyRule.DAY_MILLI_SECONDS, window.startTime);
    }
}