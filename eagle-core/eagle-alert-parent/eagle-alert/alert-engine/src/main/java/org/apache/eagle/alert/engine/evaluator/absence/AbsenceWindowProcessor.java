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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Since 7/6/16.
 * To process each incoming event
 * internally maintain state machine to trigger alert when some attribute does not occur within this window
 */
public class AbsenceWindowProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AbsenceWindowProcessor.class);
    private List<Object> expectAttrs;
    private AbsenceWindow window;
    private boolean expired; // to mark if the time range has been went through
    private OccurStatus status = OccurStatus.not_sure;

    public enum OccurStatus {
        not_sure,
        occured,
        absent
    }

    public AbsenceWindowProcessor(List<Object> expectAttrs, AbsenceWindow window) {
        this.expectAttrs = expectAttrs;
        this.window = window;
        expired = false;
    }

    /**
     * return true if it is certain that expected attributes don't occur during startTime and endTime, else return false.
     */
    public void process(List<Object> appearAttrs, long occurTime) {
        if (expired) {
            throw new IllegalStateException("Expired window can't recieve events");
        }
        switch (status) {
            case not_sure:
                if (occurTime < window.startTime) {
                    break;
                } else if (occurTime >= window.startTime
                    && occurTime <= window.endTime) {
                    if (expectAttrs.equals(appearAttrs)) {
                        status = OccurStatus.occured;
                    }
                    break;
                } else {
                    status = OccurStatus.absent;
                    break;
                }
            case occured:
                if (occurTime > window.endTime) {
                    expired = true;
                }
                break;
            default:
                break;
        }
        // reset status
        if (status == OccurStatus.absent) {
            expired = true;
        }
    }

    public OccurStatus checkStatus() {
        return status;
    }

    public boolean checkExpired() {
        return expired;
    }

    public AbsenceWindow currWindow() {
        return window;
    }

    public String toString() {
        return "expectAttrs=" + expectAttrs + ", status=" + status + ", expired=" + expired + ", window=[" + window + "]";
    }
}