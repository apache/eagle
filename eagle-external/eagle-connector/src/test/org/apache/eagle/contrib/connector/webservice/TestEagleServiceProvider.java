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
package org.apache.eagle.contrib.connector.webservice;

import junit.framework.Assert;
import org.apache.eagle.contrib.connector.policy.notification.EmailNotification;
import org.apache.eagle.contrib.connector.policy.policyobject.HDFSPolicy;
import org.apache.eagle.contrib.connector.policy.policyobject.PolicyBase;
import org.json.JSONObject;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TestEagleServiceProvider {
    EagleServiceProvider service = new EagleServiceProvider("mapr1.da.dg", "9099", "admin", "secret");
    @Test
    public void createPolicyBatch() throws Exception {
        Set<PolicyBase> set = new HashSet<PolicyBase>();
        // policy A
        HDFSPolicy hdfsPolicy1 = new HDFSPolicy();
        hdfsPolicy1.setSite("sandbox")
                .setPolicyId("testPolicyCreation1")
                .setAllowed(true)
                .setCmdEqualsTo("a")
                .setCmdRegex("d")
                .setSecurityZoneRgexp("d")
                .setSensitivityTypeEqualsTo("a")
                .setSensitivityTypeRgexp("d")
                .setSrcEqualsTo("a")
                .setSrcRgexp("d")
                .setTimestampEqualsTo(1)
                .setTimestampLessThanOrEqualTo(6)
                .setUserEqualsTo("a")
                .setUserRgexp("d");

        //construct email notification
        EmailNotification emailNotification = new EmailNotification();
        emailNotification.addSender("a@example.com")
                .addRecipient("b@example.com").addRecipient("c@example.com")
                .addSubject("test");

        hdfsPolicy1.addEmailNotification(emailNotification)
                .addKafkaNotification("sandbox.hortonworks.com:6667", "test")
                .addEagleStore().addEagleStore()
                .setDescription("test")
                .setDedupeDef(20)
                .setEnabled(true);
        set.add(hdfsPolicy1);

        // Policy B
        HDFSPolicy hdfsPolicy2 = new HDFSPolicy();
        hdfsPolicy2.setSite("sandbox")
                .setPolicyId("testPolicyCreation1")
                .setAllowed(true)
                .setCmdEqualsTo("a")
                .setCmdRegex("d")
                .setSecurityZoneRgexp("d")
                .setSensitivityTypeEqualsTo("a")
                .setSensitivityTypeRgexp("d")
                .setSrcEqualsTo("a")
                .setSrcRgexp("d")
                .setTimestampEqualsTo(1)
                .setTimestampLessThanOrEqualTo(6)
                .setUserEqualsTo("a")
                .setUserRgexp("d");

        //construct email notification
        EmailNotification emailNotification2 = new EmailNotification();
        emailNotification.addSender("a@example.com")
                .addRecipient("b@example.com").addRecipient("c@example.com")
                .addSubject("test");

        hdfsPolicy2.addEmailNotification(emailNotification2)
                .addKafkaNotification("sandbox.hortonworks.com:6667", "test")
                .addEagleStore().addEagleStore()
                .setDescription("test")
                .setDedupeDef(20)
                .setEnabled(true);
        set.add(hdfsPolicy2);

        // send both
        JSONObject jsonObject = service.createPolicyBatch(set);

        Assert.assertEquals(true, jsonObject.getBoolean("success"));

    }

    @Test
    public void getPolicy() throws Exception {
        JSONObject jsonObject = new JSONObject();
        PolicyQueryParams policyQueryParams = new PolicyQueryParams();
        //get all policies in one site
        policyQueryParams.setSite("sandbox");
        jsonObject = service.getPolicy(policyQueryParams);
        Assert.assertEquals(true, jsonObject.getBoolean("success"));
        //get all policies for applicaiton hdfsAuditLog
        policyQueryParams.setApplication("hdfsAuditLog");
        jsonObject = service.getPolicy(policyQueryParams);
        Assert.assertEquals(true, jsonObject.getBoolean("success"));

        policyQueryParams.setPolicyId("testPolicyCreation1");;
        jsonObject = service.getPolicy(policyQueryParams);
        Assert.assertEquals(true, jsonObject.getBoolean("success"));

    }


    @Test
    public void deletePolicy() throws Exception {
        createPolicyBatch();
        JSONObject jsonObject = new JSONObject();
        PolicyQueryParams policyQueryParams = new PolicyQueryParams();
        policyQueryParams.setPolicyId("testPolicyCreation1");
        jsonObject = service.deletePolicy(policyQueryParams);
        Assert.assertEquals(true, jsonObject.getBoolean("success"));
    }

    @Test
    public void setSensitivity() throws Exception {
        Map<String, Set<String>> map = new HashMap<>();
        Set<String> set = new HashSet<>();
        set.add("T1");
        set.add("T2");
        map.put("/test01", set);
        map.put("/test02", set);
        JSONObject jsonObject = service.setSensitivity("sandbox",map);
        Assert.assertEquals(true, jsonObject.getBoolean("success"));
    }

    @Test
    public void setSensitivity1() throws Exception {
        Set<String> set = new HashSet<>();
        set.add("T1");
        set.add("T2");
        JSONObject jsonObject = service.setSensitivity("sandbox","/test01", set);
        Assert.assertEquals(true, jsonObject.getBoolean("success"));
    }


    @Test
    public void removeSensitivity() throws Exception {
        setSensitivity();
        Set<String> set = new HashSet<>();
        set.add("/test01");
        set.add("/test02");
        JSONObject jsonObject = service.removeSensitivity("sandbox", set);
        Assert.assertEquals(true, jsonObject.getBoolean("success"));
    }


    @Test
    public void getSensitivityTag() throws Exception {
        setSensitivity();
        Assert.assertEquals("T1|T2", service.getSensitivityTag("sandbox", "/test01"));

    }

    @Test
    public void getAllSensitivityTag() throws Exception {
        setSensitivity();
        Map<String, String> map = new HashMap<>();
        map = service.getAllSensitivityTag("sandbox");


    }

}