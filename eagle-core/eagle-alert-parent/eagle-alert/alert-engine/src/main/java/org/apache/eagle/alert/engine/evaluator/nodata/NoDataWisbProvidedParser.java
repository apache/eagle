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
package org.apache.eagle.alert.engine.evaluator.nodata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Since 6/29/16.
 */
public class NoDataWisbProvidedParser implements NoDataWisbParser{
    @Override
    /**
     * policy value consists of "windowPeriod, type, numOfFields, f1_name, f2_name, f1_value, f2_value, f1_value, f2_value"
     */
    public Set<List<String>> parse(String[] args) {
        int numOfFields = Integer.parseInt(args[2]);
        Set<List<String>> wisbValues = new HashSet<>();
        int i = 3 + numOfFields;
        while(i<args.length){
            List<String> fields = new ArrayList<>();
            for(int j=0; j<numOfFields; j++){
                fields.add(args[i+j]);
            }
            wisbValues.add(fields);
            i += numOfFields;
        }
        return wisbValues;
    }
}
