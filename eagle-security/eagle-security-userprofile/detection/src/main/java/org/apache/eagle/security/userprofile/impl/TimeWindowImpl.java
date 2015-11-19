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
package org.apache.eagle.security.userprofile.impl;

import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.security.userprofile.TimeWindow;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @since 9/29/15
 */
public class TimeWindowImpl implements TimeWindow{
    private final Long startTimestamp;
    private final Long endTimestamp;
    private final AtomicBoolean expired;
    private final Long safeWindowMs;

    public TimeWindowImpl(Long startTimestamp, Long endTimestamp, Long safeWindowMs) {
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.expired = new AtomicBoolean(false);
        this.safeWindowMs = safeWindowMs;
    }

    public TimeWindowImpl(Long startTimestamp, Long endTimestamp) {
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.expired = new AtomicBoolean(false);
        this.safeWindowMs = 0l;
    }

    @Override
    public long from() {
        return this.startTimestamp;
    }

    @Override
    public long to() {
        return this.endTimestamp;
    }

    @Override
    public boolean accept(Long timestamp) {
        if(timestamp > endTimestamp + safeWindowMs){
            this.expired.set(true);
            return false;
        }else if(timestamp >= startTimestamp){
            return true;
        }
        return false;
    }

    @Override
    public boolean expire() {
        return expired.get();
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(this.getClass().getCanonicalName());
        builder.append(startTimestamp);
        builder.append(endTimestamp);
        return builder.toHashCode();
    }

    @Override
    public String toString() {
        return String.format("[%s - %s]",DateTimeUtil.millisecondsToHumanDateWithMilliseconds(this.startTimestamp),DateTimeUtil.millisecondsToHumanDateWithMilliseconds(this.endTimestamp));
    }
}