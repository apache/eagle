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
import org.apache.eagle.alert.engine.evaluator.absence.AbsenceAlertDriver;
import org.apache.eagle.alert.engine.evaluator.absence.AbsenceWindowGenerator;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Since 7/8/16.
 */
public class TestAbsenceDriver {
    @Test
    public void testAbsence() throws Exception{
        // from 2PM to 3PM each day
        AbsenceDailyRule rule = new AbsenceDailyRule();
        rule.startOffset = 14*3600*1000;
        rule.endOffset = 15*3600*1000;
        AbsenceWindowGenerator generator = new AbsenceWindowGenerator(rule);
        List<Object> expectAttrs = Arrays.asList("host1");
        AbsenceAlertDriver driver = new AbsenceAlertDriver(expectAttrs, generator);

        // first event came in 2016-07-08 11:20:00
        String date = "2016-07-08 11:20:00";
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date d = df.parse(date);
        long baseOccurTime = d.getTime();

        // first event
        driver.process(Arrays.asList("host2"), baseOccurTime);
        // event after 1 hour
        driver.process(Arrays.asList("host2"), baseOccurTime + 3600*1000);
        // event after 2 hour
        driver.process(Arrays.asList("host2"), baseOccurTime + 2*3600*1000);
        // event after 3 hour, enter this window
        driver.process(Arrays.asList("host2"), baseOccurTime + 3*3600*1000);
        // event after 3.5 hour, still in this window
        driver.process(Arrays.asList("host2"), baseOccurTime + 3*3600*1000 + 1800*1000);
        // event after 4 hour, exit this window
        driver.process(Arrays.asList("host2"), baseOccurTime + 4*3600*1000);
    }

    @Test
    public void testOccurrence() throws Exception{
        // from 2PM to 3PM each day
        AbsenceDailyRule rule = new AbsenceDailyRule();
        rule.startOffset = 14*3600*1000;
        rule.endOffset = 15*3600*1000;
        AbsenceWindowGenerator generator = new AbsenceWindowGenerator(rule);
        List<Object> expectAttrs = Arrays.asList("host1");
        AbsenceAlertDriver driver = new AbsenceAlertDriver(expectAttrs, generator);

        // first event came in 2016-07-08 11:20:00
        String date = "2016-07-08 11:20:00";
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date d = df.parse(date);
        long baseOccurTime = d.getTime();

        // first event
        driver.process(Arrays.asList("host2"), baseOccurTime);
        // event after 1 hour
        driver.process(Arrays.asList("host2"), baseOccurTime + 3600*1000);
        // event after 2 hour
        driver.process(Arrays.asList("host2"), baseOccurTime + 2*3600*1000);
        // event after 3 hour, enter this window
        driver.process(Arrays.asList("host2"), baseOccurTime + 3*3600*1000);
        // event after 3.5 hour, still in this window
        driver.process(Arrays.asList("host1"), baseOccurTime + 3*3600*1000 + 1800*1000);
        // event after 4 hour, exit this window
        driver.process(Arrays.asList("host2"), baseOccurTime + 4*3600*1000);
    }
}