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


public class HDFSPolicyObject extends PolicyObjectBase {

    private void checkHDFSPolicyDef(){
        if (policyDef == null){
            policyDef = new HDFSPolicyDef();
        }
    }


    //initialize hdfspolicyObject
    public HDFSPolicyObject(){
        policyTags = new HDFSPolicyTags();
    }

    public HDFSPolicyObject setSite(String site){
        policyTags.setSite(site);
        return this;
    }

    public HDFSPolicyObject setPolicyId(String policyId){
        policyTags.setPolicyId(policyId);
        return this;
    }

    //================set PolicyDef====================//
    //set allowed
    public HDFSPolicyObject setAllowed(boolean allowed){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).setAllowed(allowed);
        return this;
    }

    //set cmd
    public HDFSPolicyObject setCmdEqualsTo(String cmd){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).cmdEqualsTo(cmd);
        return this;
    }
    public HDFSPolicyObject setCmdNotEqualsTo(String cmd){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).cmdNotEqualsTo(cmd);
        return this;
    }
    public HDFSPolicyObject setCmdContains(String cmd){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).cmdContains(cmd);
        return this;
    }
    public HDFSPolicyObject setCmdRgexp(String cmd){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).cmdRegexp(cmd);
        return this;
    }

    //set dst
    public HDFSPolicyObject setDstEqualsTo(String dst){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).dstEqualsTo(dst);
        return this;
    }
    public HDFSPolicyObject setDstNotEqualsTo(String dst){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).dstNotEqualsTo(dst);
        return this;
    }
    public HDFSPolicyObject setDstContains(String dst){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).dstContains(dst);
        return this;
    }
    public HDFSPolicyObject setDstRgexp(String dst){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).dstRegexp(dst);
        return this;
    }

    //set host
    public HDFSPolicyObject setHostEqualsTo(String host){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).hostEqualsTo(host);
        return this;
    }
    public HDFSPolicyObject setHostNotEqualsTo(String host){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).hostNotEqualsTo(host);
        return this;
    }
    public HDFSPolicyObject setHostContains(String host){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).hostContains(host);
        return this;
    }
    public HDFSPolicyObject setHostRgexp(String host){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).hostRegexp(host);
        return this;
    }

    //set securityZone
    public HDFSPolicyObject setSecurityZoneEqualsTo(String securityZone){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).securityZoneEqualsTo(securityZone);
        return this;
    }
    public HDFSPolicyObject setSecurityZoneNotEqualsTo(String securityZone){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).securityZoneNotEqualsTo(securityZone);
        return this;
    }
    public HDFSPolicyObject setSecurityZoneContains(String securityZone){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).securityZoneContains(securityZone);
        return this;
    }
    public HDFSPolicyObject setSecurityZoneRgexp(String securityZone){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).securityZoneRegexp(securityZone);
        return this;
    }

    //set sensitivityType
    public HDFSPolicyObject setSensitivityTypeEqualsTo(String sensitivityType){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).sensitivityTypeEqualsTo(sensitivityType);
        return this;
    }
    public HDFSPolicyObject setSensitivityTypeNotEqualsTo(String sensitivityType){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).sensitivityTypeNotEqualsTo(sensitivityType);
        return this;
    }
    public HDFSPolicyObject setSensitivityTypeContains(String sensitivityType){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).sensitivityTypeContains(sensitivityType);
        return this;
    }
    public HDFSPolicyObject setSensitivityTypeRgexp(String sensitivityType){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).sensitivityTypeRegexp(sensitivityType);
        return this;
    }

    //set src
    public HDFSPolicyObject setSrcEqualsTo(String src){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).srcEqualsTo(src);
        return this;
    }
    public HDFSPolicyObject setSrcNotEqualsTo(String src){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).srcNotEqualsTo(src);
        return this;
    }
    public HDFSPolicyObject setSrcContains(String src){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).srcContains(src);
        return this;
    }
    public HDFSPolicyObject setSrcRgexp(String src){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).srcRegexp(src);
        return this;
    }

    //set timestamp
    public HDFSPolicyObject setTimestampEqualsTo(int timestamp){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).timestampEqualsTo(timestamp);
        return this;
    }
    public HDFSPolicyObject setTimestampNotEqualsTo(int timestamp){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).timestampNotEqualsTo(timestamp);
        return this;
    }
    public HDFSPolicyObject setTimestampLargerThan(int timestamp){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).timestampLargerThan(timestamp);
        return this;
    }
    public HDFSPolicyObject setTimestampLargerThanOrEqualTo(int timestamp){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).timestampLargerThanOrEqualsTo(timestamp);
        return this;
    }
    public HDFSPolicyObject setTimestampLessThan(int timestamp){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).timestampLessThan(timestamp);
        return this;
    }
    public HDFSPolicyObject setTimestampLessThanOrEqualTo(int timestamp){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef)policyDef).timestampLessThanOrEqualsTo(timestamp);
        return this;
    }

    //set user
    public HDFSPolicyObject setUserEqualsTo(String user){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).userEqualsTo(user);
        return this;
    }
    public HDFSPolicyObject setUserNotEqualsTo(String user){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).userNotEqualsTo(user);
        return this;
    }
    public HDFSPolicyObject setUserContains(String user){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).userContains(user);
        return this;
    }
    public HDFSPolicyObject setUserRgexp(String user){
        checkHDFSPolicyDef();
        ((HDFSPolicyDef) policyDef).userRegexp(user);
        return this;
    }

    //Override functions for adding notification and return a HDFSPolicyObject
    public HDFSPolicyObject addEmailNotificaiton(EmailNotification emailNotification){
        checkNotificationDef();
        notificationDef.addEmailNotification(emailNotification);
        return this;
    }
    public HDFSPolicyObject addKafkaNotification(String kafkaBroker, String topic){
        checkNotificationDef();
        notificationDef.addKafkaNotification(kafkaBroker, topic);
        return this;
    }
    public HDFSPolicyObject addEagleStore(){
        checkNotificationDef();
        notificationDef.addEgleStore();
        return this;
    }


}
