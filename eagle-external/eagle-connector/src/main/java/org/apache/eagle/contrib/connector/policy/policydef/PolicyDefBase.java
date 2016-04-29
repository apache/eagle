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
import org.apache.eagle.contrib.connector.policy.field.CommonField;
import org.apache.eagle.contrib.connector.policy.field.Field;
import org.apache.eagle.contrib.connector.policy.field.TimestampField;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *PolicyDefBase refers to  JSON entry "policyDef"  in policy's JSON format
 * */
public abstract class PolicyDefBase {
    /**
     *parameterHashMap contains all the matched criteria for a policy
     * */
    protected HashMap<String,Field> parameterHashMap = new HashMap<>();

    protected CommonField getCommonField(String key){
        if(!parameterHashMap.containsKey(key)){
            CommonField commonField = new CommonField(key);
            parameterHashMap.put(key,commonField);
        }
        return (CommonField) parameterHashMap.get(key);
    }

    protected void commonFieldEqualsTo(String key, String value){
        CommonField commonField = getCommonField(key);
        commonField.equalsTo(value);
    }

    protected void commonFieldNotEqualsTo(String key, String value){
        CommonField commonField = getCommonField(key);
        commonField.notEqualsTo(value);
    }

    protected void contains(String key, String value){
        CommonField commonField = getCommonField(key);
        commonField.contains(value);
    }

    protected void regexp(String key, String value){
        CommonField commonField = getCommonField(key);
        commonField.regexp(value);
    }

    protected TimestampField getTimestampField(){
        String key = PolicyConstants.TIMESTAMP;
        if(!parameterHashMap.containsKey(key)){
            TimestampField timestampField = new TimestampField();
            parameterHashMap.put(key,timestampField);
        }
        return (TimestampField) parameterHashMap.get(key);
    }

    /**
     * @return SiddhiQuery format of the policy match criterias
     * */
    public String getSiddhiQuery(){

        Iterator iterator = parameterHashMap.entrySet().iterator();
        StringBuilder policyDef = new StringBuilder();
        if(!iterator.hasNext()) return "";
        Map.Entry<String, Field> entry = (Map.Entry<String,Field>) iterator.next();

        policyDef.append(entry.getValue().getSiddhiExpression());
        while(iterator.hasNext()){
            policyDef.append(" and ");
            entry = (Map.Entry<String,Field>) iterator.next();
            policyDef.append(entry.getValue().getSiddhiExpression());
        }

        return policyDef.toString();
    }

    /**
     *@return jsonEntry  "policyDef"  in policy's JSON format
     * */
    public abstract String  toJSONEntry();
}
