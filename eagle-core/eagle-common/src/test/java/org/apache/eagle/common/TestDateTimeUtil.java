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
package org.apache.eagle.common;

import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class TestDateTimeUtil {
    @Test
    public void testRound1() {
        long tsInMS = 1397016731576L;
        long tsInMin = DateTimeUtil.roundDown(Calendar.MINUTE, tsInMS);
        Assert.assertEquals(1397016720000L, tsInMin);

        GregorianCalendar cal = new GregorianCalendar();
        cal.setTimeInMillis(tsInMS);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(tsInMin, cal.getTimeInMillis());
    }

    @Test
    public void testRound2() {
        long tsInMS = 1397016731576L;
        long tsInHour = DateTimeUtil.roundDown(Calendar.HOUR, tsInMS);
        Assert.assertEquals(1397016000000L, tsInHour);

        GregorianCalendar cal = new GregorianCalendar();
        cal.setTimeInMillis(tsInMS);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(tsInHour, cal.getTimeInMillis());
    }

    @Test
    public void testRound3() {
        long tsInMS = 1L;
        long tsInDay = DateTimeUtil.roundDown(Calendar.DATE, tsInMS);
        Assert.assertEquals(0L, tsInDay);
//		Assert.assertEquals("1970-01-01 08:00:00", DateTimeUtil.millisecondsToHumanDateWithSeconds(tsInDay));
    }

    @Test
    public void testRound4() {
        long tsInMS = 0L;
        long tsInDay = DateTimeUtil.roundDown(Calendar.DATE, tsInMS);
        Assert.assertEquals(0L, tsInDay);
        String str = DateTimeUtil.millisecondsToHumanDateWithSeconds(tsInMS);
        System.out.println(str);
    }

    @Test
    public void testRound5() {
        long tsInMS = 8 * 3600 * 1000L;
        long tsInDay = DateTimeUtil.roundDown(Calendar.DATE, tsInMS);
        Assert.assertEquals(0L, tsInDay);
        String str = DateTimeUtil.millisecondsToHumanDateWithSeconds(tsInDay);
        System.out.println(str);
    }

    @Test
    public void testDayOfWeek() {
        GregorianCalendar cal = new GregorianCalendar();
        long tsInMS = 0L;
        cal.setTimeInMillis(tsInMS);
        //cal.setTimeInMillis(System.currentTimeMillis());
        System.out.println(cal.get(Calendar.DAY_OF_WEEK));
    }

    @Test
    public void testTimeZone() throws ParseException {
        for (String s : TimeZone.getAvailableIDs()) {
            System.out.println(s);
        }
        String date = "2016-12-23 07:35:49";
        TimeZone timeZone = TimeZone.getTimeZone("GMT+8");
        long timestamp = DateTimeUtil.humanDateToSeconds(date, timeZone);
        String dateUTC = "2016-12-22 23:35:49";
        timeZone = TimeZone.getTimeZone("UTC");
        Assert.assertTrue(DateTimeUtil.secondsToHumanDate(timestamp, timeZone).equals(dateUTC));
    }
}