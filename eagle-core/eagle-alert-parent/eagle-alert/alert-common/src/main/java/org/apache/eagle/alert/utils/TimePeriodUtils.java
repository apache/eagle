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
package org.apache.eagle.alert.utils;

import org.joda.time.Period;
import org.joda.time.Seconds;

import scala.Int;

public class TimePeriodUtils {
    /**
     * For example: timestamp stands for time: 1990/01/07 12:45 and period is PT30, then result is 1990/01/07 12:30.
     *
     * @return formatted timestamp
     */
    public static long formatSecondsByPeriod(long seconds, Seconds period) {
        return seconds - (seconds % Int.int2long(period.getSeconds()));
    }

    public static long formatSecondsByPeriod(long seconds, Period period) {
        return seconds - (seconds % Int.int2long(period.toStandardSeconds().getSeconds()));
    }

    public static long formatMillisecondsByPeriod(long milliseconds, Period period) {
        return formatSecondsByPeriod(milliseconds / 1000, period) * 1000;
    }

    public static int getSecondsOfPeriod(Period period) {
        return period.toStandardSeconds().getSeconds();
    }

    public static int getMillisecondsOfPeriod(Period period) {
        return getSecondsOfPeriod(period) * 1000;
    }
}