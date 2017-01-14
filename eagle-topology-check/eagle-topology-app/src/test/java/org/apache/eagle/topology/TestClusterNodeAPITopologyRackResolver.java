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

import com.typesafe.config.ConfigFactory;
import org.apache.eagle.app.utils.connection.InputStreamUtils;
import org.apache.eagle.topology.resolver.TopologyRackResolver;
import org.apache.eagle.topology.resolver.impl.ClusterNodeAPITopologyRackResolver;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.InputStream;
import java.util.HashMap;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest(InputStreamUtils.class)
public class TestClusterNodeAPITopologyRackResolver {
    private static final String apiUrl = "http://yhd-jqhadoop168.int.yihaodian.com:8088/ws/v1/cluster/nodes";
    private static final TopologyCheckAppConfig config = TopologyCheckAppConfig.newInstance(ConfigFactory.load().withFallback(
        ConfigFactory.parseMap(new HashMap<String, String>() {{
            put("topology.resolverAPIUrl", apiUrl);
        }})));

    @Test
    public void testClusterNodeAPITopologyRackResolver() throws Exception {
        mockStatic(InputStreamUtils.class);
        String hostname = "hostname";
        mockInputSteam("/nodeinfo.json", apiUrl + "/" + hostname + ":8041");
        Class<? extends TopologyRackResolver> resolverCls = (Class<? extends TopologyRackResolver>) Class.forName("org.apache.eagle.topology.resolver.impl.ClusterNodeAPITopologyRackResolver");
        Assert.assertTrue(resolverCls == ClusterNodeAPITopologyRackResolver.class);
        TopologyRackResolver topologyRackResolver = resolverCls.newInstance();
        topologyRackResolver.prepare(config);
        Assert.assertEquals("/rowb/rack12", topologyRackResolver.resolve(hostname));
    }

    @Test
    public void testClusterNodeAPITopologyRackResolver1() throws Exception {
        mockStatic(InputStreamUtils.class);
        String hostname = "hostname";
        mockInputSteamWithException(apiUrl + "/" + hostname + ":8041");
        TopologyRackResolver topologyRackResolver = new ClusterNodeAPITopologyRackResolver();
        topologyRackResolver.prepare(config);
        Assert.assertEquals("/default-rack", topologyRackResolver.resolve(hostname));
    }

    private void mockInputSteam(String mockDataFilePath, String url) throws Exception {
        InputStream jsonstream = this.getClass().getResourceAsStream(mockDataFilePath);
        when(InputStreamUtils.getInputStream(eq(url), anyObject(), anyObject())).thenReturn(jsonstream);
    }

    private void mockInputSteamWithException(String url) throws Exception {
        when(InputStreamUtils.getInputStream(eq(url), anyObject(), anyObject())).thenThrow(new Exception());
    }
}
