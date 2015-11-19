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
package org.apache.eagle.ml.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MLCallbackResult {
	private boolean isAnomaly;
	private List<String> feature;
	private double confidence;
	private long timestamp;
	private List<String> datapoints;
	private String id;
	private String algorithmName;

    public Map<String, String> getContext() {
        return context;
    }

    public void setContext(Map<String, String> context) {
        this.context = context;
    }

    public void setAlgorithmName(String algorithmName) {
        this.algorithmName = algorithmName;
    }

    private Map<String,String> context;

	public String getAlgorithmName() {
		return algorithmName;
	}
	public void setAlgorithm(String algorithmName) {
		this.algorithmName = algorithmName;
	}
	public MLCallbackResult(){
		feature = new ArrayList<String>();
		setDatapoints(new ArrayList<String>());
	}
	public boolean isAnomaly() {
		return isAnomaly;
	}
	public void setAnomaly(boolean isAnomaly) {
		this.isAnomaly = isAnomaly;
	}
	public List<String> getFeature() {
		return feature;
	}
	public void setFeature(List<String> feature) {
		this.feature = feature;
	}

	private boolean doesFeatureExist(String f){
		boolean alreadyExist = false;
		for(String s:feature){
			if(s.equalsIgnoreCase(f))
				alreadyExist = true;
		}
		return alreadyExist;
	}
	public void setFeature(String aFeature) {
		if(doesFeatureExist(aFeature) == false)
			feature.add(aFeature);
	}
	public double getConfidence() {
		return confidence;
	}
	public void setConfidence(double confidence) {
		this.confidence = confidence;
	} 
	public String toString(){
		StringBuffer resultStr = new StringBuffer(); 
		resultStr.append("datapoint :<");
		int i=0; 
		for(String d:datapoints){
			if(i < datapoints.size()-1){
				resultStr.append(d.trim()); 
				resultStr.append(",");
				i++;
			}else
				resultStr.append(d.trim());
		}
		resultStr.append("> for user: ").append(id).append(" is anomaly: ").append(this.isAnomaly).append(" at timestamp: ").append(this.timestamp);
		if(this.isAnomaly){
			//resultStr.append(" with confidence: " + this.confidence);
			resultStr.append(" with algorithm: " + algorithmName);
			resultStr.append(" with features: ");
			int p=0;
			resultStr.append("["); 
			for(String s:feature){
				if(p < feature.size()-1){
					resultStr.append(s.trim());
					resultStr.append(",");
					p++;
				}else
					resultStr.append(s.trim());
			}
			resultStr.append("]");
		}
		return resultStr.toString();
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public List<String> getDatapoints() {
		return datapoints;
	}
	public void setDatapoints(List<String> datapoints) {
		this.datapoints = datapoints;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
}
