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
package org.apache.eagle.security.userprofile;

import org.joda.time.Period;
import org.joda.time.Seconds;
import scala.Int;

public class UserProfileUtils {
    /**
     * For example: timestamp stands for time: 1990/01/07 12:45 and period is PT30, then result is 1990/01/07 12:30
     *
     * @param seconds
     * @param period
     *
     * @return formatted timestamp
     */
    public static long formatSecondsByPeriod(long seconds,Seconds period){
        return seconds - (seconds % Int.int2long(period.getSeconds()));
    }

    /**
     * @param seconds
     * @param period
     * @return
     */
    public static long formatSecondsByPeriod(long seconds,Period period){
        return seconds - (seconds % Int.int2long(period.toStandardSeconds().getSeconds()));
    }

    /**
     * @param milliseconds
     * @param period
     * @return milliseconds
     */
    public static long formatMillisecondsByPeriod(long milliseconds,Period period){
        return formatSecondsByPeriod(milliseconds/1000,period)*1000;
    }
}