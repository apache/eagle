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
package org.apache.eagle.alert.coordination.model.internal;

/**
 *
 * This is the Base part of ScheduleState, only contains version/generateTime/code/message/scheduleTimeMillis
 *
 *
 * @since Aug 10, 2016
 *
 */
public class ScheduleStateBase {
    private String version;
    // FIXME : should be date, can not make it simple in mongo..
    private String generateTime;
    private int code = 200;
    private String message = "OK";
    private int scheduleTimeMillis;

    public ScheduleStateBase(String version, String generateTime, int code, String message, int scheduleTimeMillis) {
        this.version = version;
        this.generateTime = generateTime;
        this.code = code;
        this.message = message;
        this.scheduleTimeMillis = scheduleTimeMillis;
    }

    public int getScheduleTimeMillis() {
        return scheduleTimeMillis;
    }

    public void setScheduleTimeMillis(int scheduleTimeMillis) {
        this.scheduleTimeMillis = scheduleTimeMillis;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getGenerateTime() {
        return generateTime;
    }

    public void setGenerateTime(String generateTime) {
        this.generateTime = generateTime;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }




}
