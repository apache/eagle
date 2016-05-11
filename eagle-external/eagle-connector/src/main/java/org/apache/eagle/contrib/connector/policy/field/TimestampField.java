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
package org.apache.eagle.contrib.connector.policy.field;

import java.util.ArrayList;

/**
 * Unlike CommonField, TimestampField contains SIX different settings, including:
 * "==" , "!=" , ">", ">=", "<", "<="
 * */
public class TimestampField implements Field{

    ArrayList<String> params = new ArrayList<>();

    public void equalsTo(int timestamp){
        params.add("timestamp == " + String.valueOf(timestamp));
    }

    public void notEqualsTo(int timestamp){
        params.add("timestamp != " + String.valueOf(timestamp));
    }

    public void largerThan(int timestamp){
        params.add("timestamp > " + String.valueOf(timestamp));
    }

    public void largerThanOrEqualTo(int timestamp){
        params.add("timestamp >= " + String.valueOf(timestamp));
    }

    public void lessThan(int timestamp){
        params.add("timestamp < " + String.valueOf(timestamp));
    }

    public void lessThanOrEqualTo(int timestamp){
        params.add("timestamp <= " + String.valueOf(timestamp));
    }


    @Override
    public String getSiddhiExpression() {
        int size = params.size();
        if(size == 0)
            return null;
        else{
            StringBuilder expression = new StringBuilder();
            expression.append("(");
            expression.append(params.get(0));
            if(size > 1){
                for(int i = 1; i < size; i++){
                    expression.append(" or ");
                    expression.append(params.get(i));
                }
            }
            expression.append(")");
            return expression.toString();
        }

    }
}
