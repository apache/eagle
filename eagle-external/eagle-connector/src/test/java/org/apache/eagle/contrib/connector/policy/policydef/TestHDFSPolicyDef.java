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
package org.apache.eagle.contrib.connector.policy.policydef;

import junit.framework.Assert;
import org.junit.Test;


public class TestHDFSPolicyDef {
    @Test
    public void testAllFields(){
        HDFSPolicyDef hdfsPolicyDef = new HDFSPolicyDef();
        //set allowed
        hdfsPolicyDef.setAllowed(true);

        //set cmd
        hdfsPolicyDef.cmdEqualsTo("a");
        hdfsPolicyDef.cmdNotEqualsTo("b");
        hdfsPolicyDef.cmdContains("c");
        hdfsPolicyDef.cmdRegexp("d");

        //set dst
        hdfsPolicyDef.dstEqualsTo("a");
        hdfsPolicyDef.dstNotEqualsTo("b");
        hdfsPolicyDef.dstContains("c");
        hdfsPolicyDef.dstRegexp("d");

        //set host
        hdfsPolicyDef.hostEqualsTo("a");
        hdfsPolicyDef.hostNotEqualsTo("b");
        hdfsPolicyDef.hostContains("c");
        hdfsPolicyDef.hostRegexp("d");

        //set securityzone
        hdfsPolicyDef.securityZoneEqualsTo("a");
        hdfsPolicyDef.securityZoneNotEqualsTo("b");
        hdfsPolicyDef.securityZoneContains("c");
        hdfsPolicyDef.securityZoneRegexp("d");

        //set sensitivity type
        hdfsPolicyDef.sensitivityTypeEqualsTo("a");
        hdfsPolicyDef.sensitivityTypeNotEqualsTo("b");
        hdfsPolicyDef.sensitivityTypeContains("c");
        hdfsPolicyDef.sensitivityTypeRegexp("d");

        //set src
        hdfsPolicyDef.srcEqualsTo("a");
        hdfsPolicyDef.srcNotEqualsTo("b");
        hdfsPolicyDef.srcContains("c");
        hdfsPolicyDef.srcRegexp("d");

        //set timestamp
        hdfsPolicyDef.timestampEqualsTo(1);
        hdfsPolicyDef.timestampNotEqualsTo(2);
        hdfsPolicyDef.timestampLargerThan(3);
        hdfsPolicyDef.timestampLargerThanOrEqualsTo(4);
        hdfsPolicyDef.timestampLessThan(5);
        hdfsPolicyDef.timestampLessThanOrEqualsTo(6);

        hdfsPolicyDef.userEqualsTo("a");
        hdfsPolicyDef.userNotEqualsTo("b");
        hdfsPolicyDef.userContains("c");
        hdfsPolicyDef.userRegexp("d");

        //System.out.println(hdfsPolicyDef.getSiddhiQuery());
        Assert.assertEquals("(timestamp == 1 or timestamp != 2 or timestamp > 3 or timestamp >= 4 or timestamp < 5 or timestamp <= 6) " +
                "and (allowed == true) " +
                "and (cmd == 'a' or cmd != 'b' or str:contains(cmd,'c')==true or str:regexp(cmd,'d')==true) " +
                "and (host == 'a' or host != 'b' or str:contains(host,'c')==true or str:regexp(host,'d')==true) " +
                "and (sensitivityType == 'a' or sensitivityType != 'b' or str:contains(sensitivityType,'c')==true or str:regexp(sensitivityType,'d')==true) " +
                "and (securityZone == 'a' or securityZone != 'b' or str:contains(securityZone,'c')==true or str:regexp(securityZone,'d')==true) " +
                "and (src == 'a' or src != 'b' or str:contains(src,'c')==true or str:regexp(src,'d')==true) " +
                "and (dst == 'a' or dst != 'b' or str:contains(dst,'c')==true or str:regexp(dst,'d')==true) "+
                "and (user == 'a' or user != 'b' or str:contains(user,'c')==true or str:regexp(user,'d')==true)",hdfsPolicyDef.getSiddhiQuery());
    }
}