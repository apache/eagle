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
package org.apache.eagle.metric;

import java.util.Map;
import java.util.Map.Entry;

import com.google.common.util.concurrent.AtomicDouble;

/**
 */
public abstract class Metric implements MetricOperator{

	protected final long timestamp;
    protected final Map<String, String> dimensions;
    protected final String metricName;	
    protected final AtomicDouble value;
    
    public Metric(long timestamp, Map<String, String> dimensions, String metricName, AtomicDouble value) {
       this.timestamp = timestamp;
       this.dimensions = dimensions;
       this.metricName = metricName;
	   this.value = value;
    }
  
    public Metric(long timestamp, Map<String, String> dimensions, String metricName) {
	   this(timestamp, dimensions,metricName, new AtomicDouble(0.0));
    }

    public long getTimestamp() {
        return timestamp;
     }

     public Map<String, String> getDemensions() {
        return dimensions;
     }
   
     public String getMetricName() {
 	   return metricName;
     }
    
    public AtomicDouble getValue() {
       return value;
    }
  
    @Override
    public int hashCode() {
	   int hashCode = (int) (timestamp % Integer.MAX_VALUE);
	   for (Entry<String, String> entry : dimensions.entrySet()) {
         String key = entry.getKey();
	     String value = entry.getValue();
	     hashCode ^= key.hashCode() ^ value.hashCode();
	   }
	   return hashCode;	 
    }
  
    @Override
    public boolean equals(Object obj) {
	   if (obj instanceof Metric) {
		   Metric event = (Metric) obj;
		   if (dimensions.size() != event.dimensions.size()) return false;
		   for (Entry<String, String> keyValue : event.dimensions.entrySet()) {
		       boolean keyExist = dimensions.containsKey(keyValue.getKey());
			    if ( !keyExist || !dimensions.get(keyValue.getKey()).equals(keyValue.getValue())) {				
				    return false;
				}
	       }
		   if (timestamp != event.timestamp) return false;
		     return true;
	   }
	   return false;
    }
}
