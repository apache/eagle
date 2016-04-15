/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.service.application.entity;


public class TopologyExecutionStatus {
    public final static String STOPPED = "STOPPED";
    public final static String STARTED = "STARTED";
    public final static String STARTING = "STARTING";
    public final static String STOPPING = "STOPPING";
    public final static String NEW = "NEW";

    public static boolean isReadyToStart(String status){
        return status.equals(STOPPED) || status.equals(NEW);
    }

    public static boolean isReadyToStop(String status){
        return status.equals(STARTED);
    }

    public static String getNextStatus(String status, Boolean isSuccess) {
        switch (status) {
            case STARTED:
                if(isSuccess) return STOPPED;
                else return STARTED;
            case STOPPED:
                if(isSuccess) return STARTED;
                else return STOPPED;
            case NEW:
                if(isSuccess) return STARTED;
                else return STOPPED;
            case STARTING:
                if(isSuccess) return STARTED;
                else return STOPPED;
            case STOPPING:
                if(isSuccess) return STOPPED;
                else return STARTED;
            default: return STOPPED;
        }
    }
}

