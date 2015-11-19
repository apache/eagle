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
package eagle.auditlog.userprofile.common;

import org.apache.eagle.security.userprofile.UserProfileUtils;
import org.apache.eagle.common.DateTimeUtil;
import junit.framework.Assert;
import org.joda.time.Period;
import org.joda.time.Seconds;
import org.junit.Test;

import java.text.ParseException;

public class TestUserProfileUtils {
    @Test
    public void testJodaTimePeriod() throws ParseException {
        String periodText = "PT10m";
        Period period = new Period(periodText);
        int seconds = period.toStandardSeconds().getSeconds();
        Assert.assertEquals(600, seconds);
        Assert.assertEquals(60, period.toStandardSeconds().dividedBy(10).getSeconds());
    }

    @Test
    public void testFormatSecondsByPeriod15M() throws ParseException {

        Period period = new Period("PT15m");
        Seconds seconds = period.toStandardSeconds();
        Assert.assertEquals(15*60,seconds.getSeconds());

        long time = DateTimeUtil.humanDateToSeconds("2015-07-01 13:56:12");
        long expect = DateTimeUtil.humanDateToSeconds("2015-07-01 13:45:00");
        long result = UserProfileUtils.formatSecondsByPeriod(time,seconds);
        Assert.assertEquals(expect,result);

        time = DateTimeUtil.humanDateToSeconds("2015-07-01 03:14:59");
        expect = DateTimeUtil.humanDateToSeconds("2015-07-01 03:00:00");
        result = UserProfileUtils.formatSecondsByPeriod(time, seconds);
        Assert.assertEquals(expect,result);

        time = DateTimeUtil.humanDateToSeconds("2015-07-01 03:14:59");
        expect = DateTimeUtil.humanDateToSeconds("2015-07-01 03:00:00");
        result = UserProfileUtils.formatSecondsByPeriod(time, seconds);
        Assert.assertEquals(expect,result);
    }

    @Test
     public void testFormatSecondsByPeriod1H() throws ParseException {

        Period period = new Period("PT1h");
        Seconds seconds = period.toStandardSeconds();
        Assert.assertEquals(60*60,seconds.getSeconds());

        long time = DateTimeUtil.humanDateToSeconds("2015-07-01 13:56:12");
        long expect = DateTimeUtil.humanDateToSeconds("2015-07-01 13:00:00");
        long result = UserProfileUtils.formatSecondsByPeriod(time,seconds);
        Assert.assertEquals(expect,result);

        time = DateTimeUtil.humanDateToSeconds("2015-07-01 03:14:59");
        expect = DateTimeUtil.humanDateToSeconds("2015-07-01 03:00:00");
        result = UserProfileUtils.formatSecondsByPeriod(time, seconds);
        Assert.assertEquals(expect,result);

        time = DateTimeUtil.humanDateToSeconds("2015-07-01 03:30:59");
        expect = DateTimeUtil.humanDateToSeconds("2015-07-01 03:00:00");
        result = UserProfileUtils.formatSecondsByPeriod(time, seconds);
        Assert.assertEquals(expect,result);
    }

    /*@Test
    public void testFormatSecondsByPeriod1Min() throws ParseException {

        Period period = new Period("PT1M");
        Seconds seconds = period.toStandardSeconds();
        Assert.assertEquals(60,seconds.getSeconds());

        long time = DateTimeUtil.humanDateToSeconds("2015-07-01 13:56:15");
        long expect = DateTimeUtil.humanDateToSeconds("2015-07-01 13:55:16");
        long result = UserProfileUtils.formatSecondsByPeriod(time,seconds);
        Assert.assertEquals(expect,result);

        //time = DateTimeUtil.humanDateToSeconds("2015-07-01 03:14:59");
        //expect = DateTimeUtil.humanDateToSeconds("2015-07-01 03:00:00");
        //result = UserProfileUtils.formatSecondsByPeriod(time, seconds);
        //Assert.assertEquals(expect,result);

        //time = DateTimeUtil.humanDateToSeconds("2015-07-01 03:30:59");
        //expect = DateTimeUtil.humanDateToSeconds("2015-07-01 03:00:00");
        //result = UserProfileUtils.formatSecondsByPeriod(time, seconds);
        //Assert.assertEquals(expect,result);
    }*/
}