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
package org.apache.eagle.security.userprofile.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class UserCommandStatistics implements Serializable {
    private static final long serialVersionUID = -7145615521424420913L;
    private String commandName;
    private double mean;
    private double stddev;
    private boolean isLowVariant;

    public UserCommandStatistics(){}

    public UserCommandStatistics(Map<String, Object> rawMap){
        if(rawMap.get(COMMAND_NAME)!=null) this.commandName = (String) rawMap.get(COMMAND_NAME);
        if(rawMap.get(MEAN)!=null)this.mean = (double) rawMap.get(MEAN);
        if(rawMap.get(STDDEV)!=null) this.stddev = (double) rawMap.get(STDDEV);
        if(rawMap.get(IS_LOW_VARIANT)!=null) this.isLowVariant = (boolean) rawMap.get(IS_LOW_VARIANT);
    }

    public String getCommandName() {
        return commandName;
    }
    public void setCommandName(String commandName) {
        this.commandName = commandName;
    }

    public double getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = mean;
    }

    public double getStddev() {
        return stddev;
    }

    public void setStddev(double stddev) {
        this.stddev = stddev;
    }

    public boolean isLowVariant() {
        return isLowVariant;
    }

    public void setLowVariant(boolean isLowVariant) {
        this.isLowVariant = isLowVariant;
    }

    public final static String COMMAND_NAME = "commandName";
    public final static String MEAN = "mean";
    public final static String STDDEV = "stddev";
    public final static String IS_LOW_VARIANT = "isLowVariant";

    public Map<String,Object> toMap(){
        Map<String,Object> map = new HashMap<>();
        map.put(COMMAND_NAME,this.commandName);
        map.put(MEAN,this.mean);
        map.put(STDDEV,this.stddev);
        map.put(IS_LOW_VARIANT,this.isLowVariant);
        return map;
    }
}