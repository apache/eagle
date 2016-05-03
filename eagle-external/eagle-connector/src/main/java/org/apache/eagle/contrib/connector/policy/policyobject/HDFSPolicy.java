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

import org.apache.eagle.contrib.connector.policy.notification.EmailNotification;
import org.apache.eagle.contrib.connector.policy.policydef.HDFSPolicyDef;
import org.apache.eagle.contrib.connector.policy.tag.HDFSPolicyTags;


public class HDFSPolicy extends PolicyBase {

    private void checkHDFSPolicyDef(){
        if (policyDef == null){
            policyDef = new HDFSPolicyDef();
        }
    }
    
    //initialize hdfs policy Object
    public HDFSPolicy(){
        policyTags = new HDFSPolicyTags();
    }

    public HDFSPolicy setSite(String site){
        policyTags.setSite(site);
        return this;
    }

    public HDFSPolicy setPolicyId(String policyId){
        policyTags.setPolicyId(policyId);
        return this;
    }

    //================set PolicyDef====================//
    //set allowed
    public HDFSPolicy setAllowed(boolean allowed){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).setAllowed(allowed);
        return this;
    }

    //set cmd
    public HDFSPolicy setCmdEqualsTo(String cmd){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).cmdEqualsTo(cmd);
        return this;
    }
    public HDFSPolicy setCmdNotEqualsTo(String cmd){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).cmdNotEqualsTo(cmd);
        return this;
    }
    public HDFSPolicy setCmdContains(String cmd){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).cmdContains(cmd);
        return this;
    }
    public HDFSPolicy setCmdRegex(String cmd){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).cmdRegexp(cmd);
        return this;
    }

    //set dst
    public HDFSPolicy setDstEqualsTo(String dst){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).dstEqualsTo(dst);
        return this;
    }
    public HDFSPolicy setDstNotEqualsTo(String dst){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).dstNotEqualsTo(dst);
        return this;
    }
    public HDFSPolicy setDstContains(String dst){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).dstContains(dst);
        return this;
    }
    public HDFSPolicy setDstRegex(String dst){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).dstRegexp(dst);
        return this;
    }

    //set host
    public HDFSPolicy setHostEqualsTo(String host){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).hostEqualsTo(host);
        return this;
    }
    public HDFSPolicy setHostNotEqualsTo(String host){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).hostNotEqualsTo(host);
        return this;
    }
    public HDFSPolicy setHostContains(String host){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).hostContains(host);
        return this;
    }
    public HDFSPolicy setHostRgexp(String host){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).hostRegexp(host);
        return this;
    }

    //set securityZone
    public HDFSPolicy setSecurityZoneEqualsTo(String securityZone){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).securityZoneEqualsTo(securityZone);
        return this;
    }
    public HDFSPolicy setSecurityZoneNotEqualsTo(String securityZone){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).securityZoneNotEqualsTo(securityZone);
        return this;
    }
    public HDFSPolicy setSecurityZoneContains(String securityZone){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).securityZoneContains(securityZone);
        return this;
    }
    public HDFSPolicy setSecurityZoneRgexp(String securityZone){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).securityZoneRegexp(securityZone);
        return this;
    }

    //set sensitivityType
    public HDFSPolicy setSensitivityTypeEqualsTo(String sensitivityType){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).sensitivityTypeEqualsTo(sensitivityType);
        return this;
    }
    public HDFSPolicy setSensitivityTypeNotEqualsTo(String sensitivityType){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).sensitivityTypeNotEqualsTo(sensitivityType);
        return this;
    }
    public HDFSPolicy setSensitivityTypeContains(String sensitivityType){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).sensitivityTypeContains(sensitivityType);
        return this;
    }
    public HDFSPolicy setSensitivityTypeRgexp(String sensitivityType){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).sensitivityTypeRegexp(sensitivityType);
        return this;
    }

    //set src
    public HDFSPolicy setSrcEqualsTo(String src){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).srcEqualsTo(src);
        return this;
    }
    public HDFSPolicy setSrcNotEqualsTo(String src){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).srcNotEqualsTo(src);
        return this;
    }
    public HDFSPolicy setSrcContains(String src){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).srcContains(src);
        return this;
    }
    public HDFSPolicy setSrcRgexp(String src){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).srcRegexp(src);
        return this;
    }

    //set timestamp
    public HDFSPolicy setTimestampEqualsTo(int timestamp){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).timestampEqualsTo(timestamp);
        return this;
    }
    public HDFSPolicy setTimestampNotEqualsTo(int timestamp){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).timestampNotEqualsTo(timestamp);
        return this;
    }
    public HDFSPolicy setTimestampLargerThan(int timestamp){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).timestampLargerThan(timestamp);
        return this;
    }
    public HDFSPolicy setTimestampLargerThanOrEqualTo(int timestamp){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).timestampLargerThanOrEqualsTo(timestamp);
        return this;
    }
    public HDFSPolicy setTimestampLessThan(int timestamp){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).timestampLessThan(timestamp);
        return this;
    }
    public HDFSPolicy setTimestampLessThanOrEqualTo(int timestamp){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).timestampLessThanOrEqualsTo(timestamp);
        return this;
    }

    //set user
    public HDFSPolicy setUserEqualsTo(String user){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).userEqualsTo(user);
        return this;
    }
    public HDFSPolicy setUserNotEqualsTo(String user){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).userNotEqualsTo(user);
        return this;
    }
    public HDFSPolicy setUserContains(String user){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).userContains(user);
        return this;
    }
    public HDFSPolicy setUserRgexp(String user){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).userRegexp(user);
        return this;
    }

    //Override functions for adding notification and return a HDFSPolicy
    public HDFSPolicy addEmailNotification(EmailNotification emailNotification){
        checkNotificationDef();
        notificationDef.addEmailNotification(emailNotification);
        return this;
    }
    public HDFSPolicy addKafkaNotification(String kafkaBroker, String topic){
        checkNotificationDef();
        notificationDef.addKafkaNotification(kafkaBroker, topic);
        return this;
    }
    public HDFSPolicy addEagleStore(){
        checkNotificationDef();
        notificationDef.addEgleStore();
        return this;
    }


}
