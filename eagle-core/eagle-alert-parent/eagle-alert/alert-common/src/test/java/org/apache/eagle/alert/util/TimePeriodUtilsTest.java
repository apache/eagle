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

package org.apache.eagle.alert.util;

import org.apache.eagle.alert.utils.TimePeriodUtils;
import org.apache.eagle.common.DateTimeUtil;
import org.joda.time.Period;
import org.joda.time.Seconds;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;

public class TimePeriodUtilsTest {
    @Test
    public void testJodaTimePeriod() throws ParseException {
        String periodText = "PT10m";
        Period period = new Period(periodText);
        int seconds = period.toStandardSeconds().getSeconds();
        Assert.assertEquals(600, seconds);
        Assert.assertEquals(60, period.toStandardSeconds().dividedBy(10).getSeconds());
    }

    @Test
    public void testJodaTimePeriodBySeconds() throws ParseException {
        String periodText = "PT10s";
        Period period = new Period(periodText);
        int seconds = period.toStandardSeconds().getSeconds();
        Assert.assertEquals(10, seconds);
    }

    @Test
    public void testFormatSecondsByPeriod15M() throws ParseException {

        Period period = new Period("PT15m");
        Seconds seconds = period.toStandardSeconds();
        Assert.assertEquals(15 * 60, seconds.getSeconds());

        long time = DateTimeUtil.humanDateToSeconds("2015-07-01 13:56:12");
        long expect = DateTimeUtil.humanDateToSeconds("2015-07-01 13:45:00");
        long result = TimePeriodUtils.formatSecondsByPeriod(time, seconds);
        Assert.assertEquals(expect, result);

        time = DateTimeUtil.humanDateToSeconds("2015-07-01 03:14:59");
        expect = DateTimeUtil.humanDateToSeconds("2015-07-01 03:00:00");
        result = TimePeriodUtils.formatSecondsByPeriod(time, seconds);
        Assert.assertEquals(expect, result);

        time = DateTimeUtil.humanDateToSeconds("2015-07-01 03:14:59");
        expect = DateTimeUtil.humanDateToSeconds("2015-07-01 03:00:00");
        result = TimePeriodUtils.formatSecondsByPeriod(time, seconds);
        Assert.assertEquals(expect, result);
    }

    @Test
    public void testFormatSecondsByPeriod1H() throws ParseException {

        Period period = new Period("PT1h");
        Seconds seconds = period.toStandardSeconds();
        Assert.assertEquals(60 * 60, seconds.getSeconds());

        long time = DateTimeUtil.humanDateToSeconds("2015-07-01 13:56:12");
        long expect = DateTimeUtil.humanDateToSeconds("2015-07-01 13:00:00");
        long result = TimePeriodUtils.formatSecondsByPeriod(time, seconds);
        Assert.assertEquals(expect, result);

        time = DateTimeUtil.humanDateToSeconds("2015-07-01 03:14:59");
        expect = DateTimeUtil.humanDateToSeconds("2015-07-01 03:00:00");
        result = TimePeriodUtils.formatSecondsByPeriod(time, seconds);
        Assert.assertEquals(expect, result);

        time = DateTimeUtil.humanDateToSeconds("2015-07-01 03:30:59");
        expect = DateTimeUtil.humanDateToSeconds("2015-07-01 03:00:00");
        result = TimePeriodUtils.formatSecondsByPeriod(time, seconds);
        Assert.assertEquals(expect, result);
    }


    @Test
    public void testPeriod() {
        Assert.assertEquals(30 * 60 * 1000, TimePeriodUtils.getMillisecondsOfPeriod(Period.parse("PT30m")));
        Assert.assertEquals(30 * 60 * 1000, TimePeriodUtils.getMillisecondsOfPeriod(Period.millis(30 * 60 * 1000)));
        Assert.assertEquals("PT1800S", Period.millis(30 * 60 * 1000).toString());
    }
}