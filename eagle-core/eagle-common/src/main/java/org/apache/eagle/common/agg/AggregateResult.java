/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  * <p/>
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  * <p/>
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.common.agg;

import java.util.List;
import java.util.Map;

/**
 * Since 8/4/16.
 * schema is : gbfield, gbfield, ..., aggField, aggField, ...
 */
public class AggregateResult {
    private Object[] data;
    private Map<String, Integer> colIndices;
    private List<String> colNames;
    public AggregateResult(Object[] data, Map<String, Integer> colIndices, List<String> colNames){
        this.data = data;
        this.colIndices = colIndices;
        this.colNames = colNames;
    }

    public Object get(int index){
        return data[index];
    }

    public Object get(String fieldName){
        int index = colIndices.get(fieldName);
        return get(index);
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for(int i=0; i<data.length; i++){
            sb.append(colNames.get(i));
            sb.append("=");
            sb.append(data[i]);
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("}");
        return sb.toString();
    }
}
