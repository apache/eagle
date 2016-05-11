/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */
package org.apache.eagle.contrib.connector.policy.policydef;

import org.apache.eagle.contrib.connector.policy.common.PolicyConstants;
import org.apache.eagle.contrib.connector.policy.field.AllowedField;
import org.apache.eagle.contrib.connector.policy.field.TimestampField;

public class HDFSPolicyDef extends PolicyDefBase{





    //for allowed
    public void setAllowed(boolean allowed){
        String key = PolicyConstants.HDFS_ALLOWED;
        if(!parameterHashMap.containsKey(key)){
            AllowedField allowedField = new AllowedField();
            parameterHashMap.put(key,allowedField);
        }
        ((AllowedField)parameterHashMap.get(key)).setAllowed(allowed);
    }
    private AllowedField getAllowedField(){
        String key = PolicyConstants.HDFS_ALLOWED;
        if(!parameterHashMap.containsKey(key)){
            AllowedField allowedField = new AllowedField();
            parameterHashMap.put(key,allowedField);
        }
        return (AllowedField)parameterHashMap.get(key);
    }



    //For cmd
    public void cmdEqualsTo(String value){
        commonFieldEqualsTo(PolicyConstants.HDFS_COMMAND,value);
    }
    public void cmdNotEqualsTo(String value){
        commonFieldNotEqualsTo(PolicyConstants.HDFS_COMMAND,value);
    }
    public void cmdContains(String value){
        contains(PolicyConstants.HDFS_COMMAND,value);
    }
    public void cmdRegexp(String value){
        regexp(PolicyConstants.HDFS_COMMAND,value);
    }

    //For dst
    public void dstEqualsTo(String value){
        commonFieldEqualsTo(PolicyConstants.HDFS_DESTINATION,value);
    }
    public void dstNotEqualsTo(String value){
        commonFieldNotEqualsTo(PolicyConstants.HDFS_DESTINATION,value);
    }
    public void dstContains(String value){
        contains(PolicyConstants.HDFS_DESTINATION,value);
    }
    public void dstRegexp(String value){
        regexp(PolicyConstants.HDFS_DESTINATION,value);
    }

    //For host
    public void hostEqualsTo(String value){
        commonFieldEqualsTo(PolicyConstants.HOST,value);
    }
    public void hostNotEqualsTo(String value){
        commonFieldNotEqualsTo(PolicyConstants.HOST,value);
    }
    public void hostContains(String value){
        contains(PolicyConstants.HOST,value);
    }
    public void hostRegexp(String value){
        regexp(PolicyConstants.HOST,value);
    }

    //For securityZone
    public void securityZoneEqualsTo(String value){
        commonFieldEqualsTo(PolicyConstants.SECURITYZONE,value);
    }
    public void securityZoneNotEqualsTo(String value){
        commonFieldNotEqualsTo(PolicyConstants.SECURITYZONE,value);
    }
    public void securityZoneContains(String value){
        contains(PolicyConstants.SECURITYZONE,value);
    }
    public void securityZoneRegexp(String value){
        regexp(PolicyConstants.SECURITYZONE,value);
    }

    //For sensitivityType
    public void sensitivityTypeEqualsTo(String value){
        commonFieldEqualsTo(PolicyConstants.HDFS_SENSITIVITY_TYPE,value);
    }
    public void sensitivityTypeNotEqualsTo(String value){
        commonFieldNotEqualsTo(PolicyConstants.HDFS_SENSITIVITY_TYPE,value);
    }
    public void sensitivityTypeContains(String value){
        contains(PolicyConstants.HDFS_SENSITIVITY_TYPE,value);
    }
    public void sensitivityTypeRegexp(String value){
        regexp(PolicyConstants.HDFS_SENSITIVITY_TYPE,value);
    }

    //For src
    public void srcEqualsTo(String value){
        commonFieldEqualsTo(PolicyConstants.HDFS_SOURCE,value);
    }
    public void srcNotEqualsTo(String value){
        commonFieldNotEqualsTo(PolicyConstants.HDFS_SOURCE,value);
    }
    public void srcContains(String value){
        contains(PolicyConstants.HDFS_SOURCE,value);
    }
    public void srcRegexp(String value){
        regexp(PolicyConstants.HDFS_SOURCE,value);
    }

    //For user
    public void userEqualsTo(String value){
        commonFieldEqualsTo(PolicyConstants.USER,value);
    }
    public void userNotEqualsTo(String value){
        commonFieldNotEqualsTo(PolicyConstants.USER,value);
    }
    public void userContains(String value){
        contains(PolicyConstants.USER,value);
    }
    public void userRegexp(String value){
        regexp(PolicyConstants.USER, value);
    }


    //For timestamp
    public void timestampEqualsTo(int timestamp){
        TimestampField timestampField = getTimestampField();
        timestampField.equalsTo(timestamp);
    }

    public void timestampNotEqualsTo(int timestamp){
        TimestampField timestampField = getTimestampField();
        timestampField.notEqualsTo(timestamp);
    }

    public void timestampLargerThan(int timestamp){
        TimestampField timestampField = getTimestampField();
        timestampField.largerThan(timestamp);
    }

    public void timestampLargerThanOrEqualsTo(int timestamp){
        TimestampField timestampField = getTimestampField();
        timestampField.largerThanOrEqualTo(timestamp);
    }

    public void timestampLessThan(int timestamp){
        TimestampField timestampField = getTimestampField();
        timestampField.lessThan(timestamp);
    }

    public void timestampLessThanOrEqualsTo(int timestamp){
        TimestampField timestampField = getTimestampField();
        timestampField.lessThanOrEqualTo(timestamp);
    }

    @Override
    public String toJSONEntry(){
        StringBuilder jsonEntry = new StringBuilder(100);
        jsonEntry.append("\"policyDef\":")
                .append("\"{\\\"type\\\":")
                .append("\\\"" + PolicyConstants.HDFS_POLICYTYPE + "\\\"").append(",")
                .append("\\\"expression\\\":")
                .append("\\\"from " + PolicyConstants.HDFS_AUDITLOG_EVENT_STREAM + "[")
                .append(getSiddhiQuery())
                .append("]")
                .append(" select * insert into outputStream;\\\"")
                .append("}\"");
        return jsonEntry.toString();
    }


}


