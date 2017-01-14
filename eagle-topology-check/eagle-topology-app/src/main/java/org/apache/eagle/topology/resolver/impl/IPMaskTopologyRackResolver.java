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

package org.apache.eagle.topology.resolver.impl;

import org.apache.eagle.topology.resolver.TopologyRackResolver;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * resolve rack by hostname.
 */
public class IPMaskTopologyRackResolver implements TopologyRackResolver {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(IPMaskTopologyRackResolver.class);

    private final int pos = 2;
    private int rackPos;

    public IPMaskTopologyRackResolver() {
        this.rackPos = pos;
    }

    public IPMaskTopologyRackResolver(int rackPos) {
        this.rackPos = (rackPos > 3 || rackPos < 0) ? pos : rackPos;
    }

    @Override
    public String resolve(String hostname) {
        String result = "null";
        try {
            InetAddress address = InetAddress.getByName(hostname);
            result = "rack" + (int) (address.getAddress()[rackPos] & 0xff);
        } catch (UnknownHostException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("UnknownHostException: {}", hostname);
            }
        }
        return result;
    }
}
