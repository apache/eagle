/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.topology.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class EntityBuilderHelper {

    public static String resolveRackByHost(String hostname) {
        String result = null;
        try {
            InetAddress address = InetAddress.getByName(hostname);
            result = "rack" + (int)(address.getAddress()[2] & 0xff);
        } catch (UnknownHostException e) {
//			final String errMsg = "Got an EagleServiceClientException when trying to refresh host/rack cache: " + e.getMessage();
//			LOGGER.error(errMsg, e);
        }
        return result;
    }
}
