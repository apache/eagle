/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.util.jobcounter;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;


public final class JobCounters implements Serializable {
    
    private Map<String, Map<String, Long>> counters = new TreeMap<>();

    public Map<String, Map<String, Long>> getCounters() {
        return counters;
    }

    public void setCounters(Map<String, Map<String, Long>> counters) {
        this.counters = counters;
    }
    
    public String toString() {
        return counters.toString();
    }

    public void clear() {
        for (Map.Entry<String, Map<String, Long>> entry : counters.entrySet()) {
            entry.getValue().clear();
        }
        counters.clear();
    }
}
