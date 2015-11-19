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
import org.apache.eagle.common.config.EagleConfigFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * be aware that SimpleDateFormat instantiation is expensive, so if that's under a tight loop, probably we need
 * a thread local SimpleDateFormat object
 */
public class DateTimeUtil {
	public static final long ONESECOND = 1L * 1000L;
	public static final long ONEMINUTE = 1L * 60L * 1000L;
	public static final long ONEHOUR = 1L * 60L * 60L * 1000L;
	public static final long ONEDAY = 24L * 60L * 60L * 1000L;
    private static TimeZone CURRENT_TIME_ZONE = EagleConfigFactory.load().getTimeZone();
	
	public static Date humanDateToDate(String date) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(CURRENT_TIME_ZONE);
		return sdf.parse(date);
	}
	
	public static String secondsToHumanDate(long seconds){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(CURRENT_TIME_ZONE);
		Date t = new Date();
		t.setTime(seconds*1000);
		return sdf.format(t);
	}
	
	public static String millisecondsToHumanDateWithMilliseconds(long milliseconds){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
        sdf.setTimeZone(CURRENT_TIME_ZONE);
		Date t = new Date();
		t.setTime(milliseconds);
		return sdf.format(t);
	}
	
	public static String millisecondsToHumanDateWithSeconds(long milliseconds){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        sdf.setTimeZone(CURRENT_TIME_ZONE);
		Date t = new Date();
		t.setTime(milliseconds);
		return sdf.format(t);
	}
	
	public static long humanDateToSeconds(String date) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(CURRENT_TIME_ZONE);
		Date d = sdf.parse(date);
		return d.getTime()/1000;
	}
	
	public static long humanDateToMilliseconds(String date) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
        sdf.setTimeZone(CURRENT_TIME_ZONE);
		Date d = sdf.parse(date);
		return d.getTime();
	}
	
	
	public static long humanDateToMillisecondsWithoutException(String date){
		try{
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            sdf.setTimeZone(CURRENT_TIME_ZONE);
			Date d = sdf.parse(date);
			return d.getTime();
		}catch(ParseException ex){
			return 0L;
		}
	}
	
	public static long humanDateToSecondsWithoutException(String date){
		try{
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            sdf.setTimeZone(CURRENT_TIME_ZONE);
			Date d = sdf.parse(date);
			return (d.getTime() / 1000);
		}catch(ParseException ex){
			return 0L;
		}
	}
	/**
	 * this could be accurate only when timezone is UTC
	 * for the timezones other than UTC, there is possibly issue, for example
	 * assume timezone is GMT+8 in China
	 * When user time is "2014-07-15 05:00:00", it will be converted to timestamp first, internally it would be  "2014-07-14 21:00:00" in UTC timezone. When rounded down to day, the internal time would 
	 * be changed to "2014-07-14 00:00:00", and that means the user time is "2014-07-14 08:00:00". But originally user wants to round it to "2014-07-15 00:00:00"
	 * 
	 * @param field
	 * @param timeInMillis the seconds elapsed since 1970-01-01 00:00:00
	 * @return
	 */
	public static long roundDown(int field, long timeInMillis){
		switch(field){
			case Calendar.DAY_OF_MONTH:
			case Calendar.DAY_OF_WEEK:
			case Calendar.DAY_OF_YEAR:
				return (timeInMillis - timeInMillis % (24*60*60*1000));
			case Calendar.HOUR:
				return (timeInMillis - timeInMillis % (60*60*1000));
			case Calendar.MINUTE:
				return (timeInMillis - timeInMillis % (60*1000));
			case Calendar.SECOND:
				return (timeInMillis - timeInMillis % (1000));
			default:
				return 0L;
		}
	}

	public static String format(long milliseconds, String format) {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(CURRENT_TIME_ZONE);
		Date t = new Date();
		t.setTime(milliseconds);
		return sdf.format(t);
	}
}
