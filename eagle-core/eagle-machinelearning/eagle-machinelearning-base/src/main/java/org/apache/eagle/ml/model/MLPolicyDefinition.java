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

import org.apache.eagle.alert.config.AbstractPolicyDefinition;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Properties;

/**
 {  
   "type":"MachineLearning",
   "context":{
      "site":"dev",
      "dataSource":"userprofile"
      "component":"dev-component",
      "description":"ML based user profile anomaly detection",
      "severity":"WARNING",
      "notificationByEmail":"true"
   },
   "algorithms":[
      {
         "name":"eagle.security.userprofile.util.EigenBasedAnomalyDetection",
         "description":"EigenBasedAnomalyDetection",
         "features":"getfileinfo, open, listStatus, setTimes, setPermission, rename, mkdirs, create, setReplication, contentSummary, delete"
      },
      {  
         "name":"eagle.security.userprofile.util.KDEBasedAnomalyDetection",
         "description":"DensityBasedAnomalyDetection",
         "features":"getfileinfo, open, listStatus, setTimes, setPermission, rename, mkdirs, create, setReplication, contentSummary, delete"
      }
   ]
}
   version field is used for model update, so eagle framework can understand that something changes.

 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", visible=true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class MLPolicyDefinition extends AbstractPolicyDefinition{
	private String version;
	private Properties context;
    private MLAlgorithm[] algorithms;

    public MLAlgorithm[] getAlgorithms() {
        return algorithms;
    }

    public void setAlgorithms(MLAlgorithm[] algorithms) {
        this.algorithms = algorithms;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Properties getContext() {
        return context;
    }

    public void setContext(Properties context) {
        this.context = context;
    }
}