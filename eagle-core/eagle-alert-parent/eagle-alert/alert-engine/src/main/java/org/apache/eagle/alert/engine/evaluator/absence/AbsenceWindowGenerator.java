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
package org.apache.eagle.alert.engine.evaluator.absence;

/**
 * Since 7/7/16.
 */
public class AbsenceWindowGenerator {
    private AbsenceRule absenceRule;

    public AbsenceWindowGenerator(AbsenceRule rule) {
        this.absenceRule = rule;
    }

    /**
     * nextWindow.
     *
     * @param currTime current timestamp
     */
    public AbsenceWindow nextWindow(long currTime) {
        AbsenceWindow window = new AbsenceWindow();
        if (absenceRule instanceof AbsenceDailyRule) {
            AbsenceDailyRule rule = (AbsenceDailyRule) absenceRule;
            long modDailyTime = currTime % AbsenceDailyRule.DAY_MILLI_SECONDS;
            // use current timestamp to round down to day
            long day = currTime - modDailyTime;
            // if today's window already expires, then adjust to tomorrow's window
            if (modDailyTime > rule.startOffset) {
                day += AbsenceDailyRule.DAY_MILLI_SECONDS;
            }
            window.startTime = day + rule.startOffset;
            window.endTime = day + rule.endOffset;
            return window;
        } else if (absenceRule instanceof AbsenceHourlyRule) {
            AbsenceHourlyRule rule = (AbsenceHourlyRule) absenceRule;
            long modTime = (currTime - rule.getStartTime()) % rule.getInterval();
            long newStartTime = currTime - modTime;
            if (modTime > 0) {
                newStartTime += rule.getInterval();
            }
            window.startTime = newStartTime;
            window.endTime = newStartTime + rule.getEndTime() - rule.getStartTime();
            return window;
        } else {
            throw new UnsupportedOperationException("Not supported absenceRule " + absenceRule);
        }
    }
}