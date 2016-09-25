/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.topology;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class TestNetMaskTopologyRackResolver {

    @Test
    public void test() {
        String hostname = "192.168.1.4";
        int subnet = 24;
        String result = null;
        try {
            InetAddress address = InetAddress.getByName(hostname);
            int netmask = (subnet % 8) ^ 0xff;
            int ipAddr = subnet / 8;
            result = "rack" + (int)(address.getAddress()[ipAddr] & netmask);
        } catch (UnknownHostException e) {
            //e.printStackTrace();
        }
        Assert.assertTrue(result.equals("rack4"));

    }

    @Test
    public void test2() {
        String hostname = "192.168.1.4";
        String netmask = "255.255.255.0";
        String result = null;
        try {
            InetAddress address = InetAddress.getByName(hostname);
            InetAddress maskAddr = InetAddress.getByName(netmask);
            int rack = 0;
            for (int i = 0; i < 4; i++) {
                int tmp = maskAddr.getAddress()[i] & 0xff;
                rack = address.getAddress()[i] & (tmp ^ 0xff);
                if (rack != 0) break;
            }
            result = "rack" + rack;
        } catch (UnknownHostException e) {
            //e.printStackTrace();
        }
        Assert.assertTrue(result.equals("rack4"));

    }
}
