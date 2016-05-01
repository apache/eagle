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
package org.apache.eagle.contrib.connector.policy.policyobject;

import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.contrib.connector.policy.common.PolicyConstants;
import org.apache.eagle.contrib.connector.policy.notification.EmailNotification;
import org.apache.eagle.contrib.connector.policy.notification.NotificationDef;
import org.apache.eagle.contrib.connector.policy.policydef.PolicyDefBase;
import org.apache.eagle.contrib.connector.policy.tag.PolicyTagsBase;

import java.util.ArrayList;


public abstract class PolicyObjectBase {
    /**
     * poliyTags contains "site, application, policyId, alertExecutorId, policyType"
     * */
    protected   PolicyTagsBase policyTags;
    protected   PolicyDefBase policyDef;
    protected   NotificationDef notificationDef;


    /**
     * description of this policy
     * */
    protected String description;

    /**
     * enable this policy or not
     * */
    protected boolean enabled = true;

    /**
     * Alert de-dup interval, in minutes
     * */
    protected int dedupeDef;

    //crated notification if not existed
    protected void checkNotificationDef(){
        if(notificationDef == null) {
            notificationDef = new NotificationDef();
        }
    }

    public PolicyObjectBase setDescription(String description){
        this.description = description;
        return this;
    }
    public String getDescription(){
        return description;
    }


    public PolicyObjectBase setEnabled(boolean enabled){
        this.enabled = enabled;
        return this;
    }
    public boolean isEnabled(){
        return enabled;
    }


    public PolicyObjectBase setDedupeDef(int min){
        this.dedupeDef = min;
        return this;
    }
    public int getDedupeDef(){
        return this.dedupeDef;
    }


// Need to be override  to return child object
    public PolicyObjectBase addEmailNotification(EmailNotification emailNotification){
        checkNotificationDef();
        notificationDef.addEmailNotification(emailNotification);
        return this;
    }
    public PolicyObjectBase addKafkaNotification(String kafkaBroker, String topic){
        checkNotificationDef();
        notificationDef.addKafkaNotification(kafkaBroker, topic);
        return this;
    }
    public PolicyObjectBase addEagleStore(){
        checkNotificationDef();
        notificationDef.addEgleStore();
        return this;
    }

    /**
     * @return json format of this policy
     * */
    public String toJSONData(){
        //tags, desc, policyDef, enabled
        ArrayList<String> policyEntries = new ArrayList<>();
        if(policyTags != null) policyEntries.add(policyTags.toJSONEntry());
        if(description != null) policyEntries.add("\"description\":\"" + description + "\"");
        if(policyDef != null) policyEntries.add(policyDef.toJSONEntry());
        if(notificationDef != null) policyEntries.add(notificationDef.toJSONEntry());

        policyEntries.add("\"dedupeDef\":\"{\\\"" + PolicyConstants.ALERT_DEDUP_INTERVAL_MIN + "\\\":" + String.valueOf(dedupeDef) + "}\"");
        policyEntries.add("\"enabled\":"+String.valueOf(enabled));

        String value = StringUtils.join(policyEntries,",");
        StringBuilder jsonData = new StringBuilder(200);
        jsonData.append("[{").append(value).append("}]");

        return jsonData.toString();
    }







}
