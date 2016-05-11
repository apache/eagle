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
import org.junit.Test;

public class TestHDFSPolicyObject {
    @Test
    public void TestCreateHDFSPolicyObject(){
        HDFSPolicyObject hdfsPolicyObject = new HDFSPolicyObject();
        hdfsPolicyObject.setSite("sandbox")
                .setPolicyId("testHDFSPolicyCreation")
                .setAllowed(true)
                .setCmdEqualsTo("a")
                .setCmdNotEqualsTo("b")
                .setCmdContains("c")
                .setCmdRgexp("d")
                .setDstEqualsTo("a")
                .setDstNotEqualsTo("b")
                .setDstContains("c")
                .setDstRgexp("d")
                .setHostEqualsTo("a")
                .setHostNotEqualsTo("b")
                .setHostContains("c")
                .setHostRgexp("d")
                .setSecurityZoneEqualsTo("a")
                .setSecurityZoneNotEqualsTo("b")
                .setSecurityZoneContains("c")
                .setSecurityZoneRgexp("d")
                .setSensitivityTypeEqualsTo("a")
                .setSensitivityTypeNotEqualsTo("b")
                .setSensitivityTypeContains("c")
                .setSensitivityTypeRgexp("d")
                .setSrcEqualsTo("a")
                .setSrcNotEqualsTo("b")
                .setSrcContains("c")
                .setSrcRgexp("d")
                .setTimestampEqualsTo(1)
                .setTimestampNotEqualsTo(2)
                .setTimestampLargerThan(3)
                .setTimestampLargerThanOrEqualTo(4)
                .setTimestampLessThan(5)
                .setTimestampLessThanOrEqualTo(6)
                .setUserEqualsTo("a")
                .setUserNotEqualsTo("b")
                .setUserContains("c")
                .setUserRgexp("d");

        //construct email notification
        EmailNotification emailNotification = new EmailNotification();
        emailNotification.addSender("a@example.com")
                .addRecipient("b@example.com").addRecipient("c@example.com")
                .addSubject("test");

        hdfsPolicyObject.addEmailNotificaiton(emailNotification)
                        .addKafkaNotification("sandbox.hortonworks.com:6667", "test")
                        .addEagleStore().addEagleStore()
                        .setDescription("test")
                        .setEnabled(true);

        System.out.println(hdfsPolicyObject.toJSONData());


    }
}