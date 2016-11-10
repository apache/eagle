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
package org.apache.eagle.alert.engine.coordinator;

import org.apache.commons.lang.StringUtils;
import org.apache.eagle.alert.utils.TimePeriodUtils;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.joda.time.Period;

import java.io.Serializable;

/**
 * streamId is the key.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamSortSpec implements Serializable {
    private static final long serialVersionUID = 3626506441441584937L;
    private String windowPeriod = "";
    private int windowMargin = 30 * 1000; // 30 seconds by default

    public StreamSortSpec() {
    }

    public StreamSortSpec(StreamSortSpec spec) {
        this.windowPeriod = spec.windowPeriod;
        this.windowMargin = spec.windowMargin;
    }

    public String getWindowPeriod() {
        return windowPeriod;
    }

    public int getWindowPeriodMillis() {
        if (StringUtils.isNotBlank(windowPeriod)) {
            return TimePeriodUtils.getMillisecondsOfPeriod(Period.parse(windowPeriod));
        } else {
            return 0;
        }
    }

    public void setWindowPeriod(String windowPeriod) {
        this.windowPeriod = windowPeriod;
    }

    public void setWindowPeriodMillis(int windowPeriodMillis) {
        this.windowPeriod = Period.millis(windowPeriodMillis).toString();
    }

    public void setWindowPeriod2(Period period) {
        this.windowPeriod = period.toString();
    }


    public int getWindowMargin() {
        return windowMargin;
    }

    public void setWindowMargin(int windowMargin) {
        this.windowMargin = windowMargin;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(windowPeriod)
                .append(windowMargin)
                .toHashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (!(that instanceof StreamSortSpec)) {
            return false;
        }

        StreamSortSpec another = (StreamSortSpec) that;
        return
                another.windowPeriod.equals(this.windowPeriod)
                        && another.windowMargin == this.windowMargin;
    }

    @Override
    public String toString() {
        return String.format("StreamSortSpec[windowPeriod=%s,windowMargin=%d]",
                this.getWindowPeriod(),
                this.getWindowMargin());
    }
}