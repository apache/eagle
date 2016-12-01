/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.service;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.collections.map.HashedMap;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.apache.eagle.alert.metadata.impl.InMemMetadataDaoImpl;
import org.apache.eagle.alert.metric.MetricConfigs;
import org.apache.eagle.app.Application;
import org.apache.eagle.app.environment.impl.StaticEnvironment;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.SiteEntity;
import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ApplicationActionTest {
    /**
     * appConfig.withFallback(envConfig): appConfig will override envConfig, envConfig is used as default config
     */
    @Test
    public void testTypeSafeConfigMerge(){
        Config appConfig = ConfigFactory.parseMap(new HashMap<String,String>(){{
            put("APP_CONFIG",ApplicationActionTest.this.getClass().getCanonicalName());
            put("SCOPE","APP");
        }});

        Config envConfig = ConfigFactory.parseMap(new HashMap<String,String>(){{
            put("ENV_CONFIG",ApplicationActionTest.this.getClass().getCanonicalName());
            put("SCOPE","ENV");
        }});

        Config mergedConfig = appConfig.withFallback(envConfig);
        Assert.assertTrue(mergedConfig.hasPath("APP_CONFIG"));
        Assert.assertTrue(mergedConfig.hasPath("ENV_CONFIG"));
        Assert.assertEquals("appConfig.withFallback(envConfig): appConfig will override envConfig, envConfig is used as default config",
                "APP",mergedConfig.getString("SCOPE"));
    }

    @Test
    public void testDoAction() {
        Application application = mock(Application.class);
        when(application.getEnvironmentType()).thenReturn(StaticEnvironment.class);
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        ApplicationDesc applicationDesc = new ApplicationDesc();
        List<StreamDefinition> streamDefinitions = new ArrayList<>();
        StreamDefinition sd = new StreamDefinition();
        sd.setStreamId("streamId");
        sd.setDescription("desc");
        sd.setValidate(true);
        sd.setTimeseries(false);
        sd.setDataSource("ds1");
        sd.setSiteId("siteId");
        streamDefinitions.add(sd);
        applicationDesc.setStreams(streamDefinitions);
        applicationDesc.setType("type1");
        ApplicationEntity metadata = new ApplicationEntity();
        metadata.setAppId("appId");
        metadata.setSite(siteEntity);
        metadata.setDescriptor(applicationDesc);
        metadata.setMode(ApplicationEntity.Mode.LOCAL);
        metadata.setJarPath(applicationDesc.getJarPath());
        Map<String, Object> configure = new HashedMap();
        configure.put("dataSinkConfig.topic", "test_topic");
        configure.put("dataSinkConfig.brokerList", "sandbox.hortonworks.com:6667");
        configure.put(MetricConfigs.METRIC_PREFIX_CONF, "eagle.");
        metadata.setConfiguration(configure);
        metadata.setContext(configure);
        Config serverConfig = ConfigFactory.parseMap(new HashMap<String,String>(){{
            put("coordinator.metadataService.host", "localhost");
            put("coordinator.metadataService.context", "/rest");
        }});
        IMetadataDao alertMetadataService = new InMemMetadataDaoImpl(serverConfig);
        ApplicationAction applicationAction = new ApplicationAction(application, metadata, serverConfig, alertMetadataService);
        applicationAction.doInstall();
        applicationAction.doUninstall();
        applicationAction.doStart();
        applicationAction.doStop();
        Assert.assertEquals(ApplicationEntity.Status.INITIALIZED, applicationAction.getStatus());
    }
}