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
package org.apache.eagle.contrib.connector.policy.tag;

import org.slf4j.Logger;
/**
 * Refers to json entry "tags" in policy's json format
 * */
public class PolicyTagsBase {
    public final static Logger LOG = org.slf4j.LoggerFactory.getLogger(PolicyTagsBase.class);
    protected String site;
    protected String application;
    protected String policyId;
    protected String alertExecutorId;
    protected String policyType;

    public void setSite(String site){
        this.site = site;
    }

    public String getSite(){
        return site;
    }

    public void setPolicyId(String policyId){
        this.policyId = policyId;
    }

    public String getPolicyId(String policyId){
        return policyId;
    }

    public void setApplication(String application){
        this.application = application;
    }

    public String getApplication(){
        return application;
    }

    public void setAlertExecutorId(){
        this.alertExecutorId = alertExecutorId;
    }

    public String getAlertExecutorId(){
        return alertExecutorId;
    }

    public String toJSONEntry(){
        if(site == null || policyId == null || application == null) LOG.info("Invalid policy created. Valid policy must set site, application and policyId ");
        StringBuilder tags = new StringBuilder(100);
        tags.append("\"tags\":{");
        tags.append("\"site\":").append("\""+site+"\",")
                .append("\"application\":").append("\""+ application +"\",")
                .append("\"policyId\":").append("\""+policyId+"\",")
                .append("\"alertExecutorId\":").append("\""+alertExecutorId+"\",")
                .append("\"policyType\":").append("\""+policyType+"\"")
                .append("}");
        return tags.toString();
    }
}
