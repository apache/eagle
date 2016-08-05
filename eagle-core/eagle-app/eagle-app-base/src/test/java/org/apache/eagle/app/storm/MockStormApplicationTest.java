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
package org.apache.eagle.app.storm;

import com.typesafe.config.ConfigFactory;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.app.utils.DynamicJarPathFinder;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class MockStormApplicationTest {
    @Test
    public void testGetConfigClass(){
        MockStormApplication mockStormApplication = new MockStormApplication();
        Assert.assertEquals(MockStormApplication.MockStormConfiguration.class,mockStormApplication.getConfigType());
    }

    @Test
    public void testGetConfigFromMap(){
        MockStormApplication mockStormApplication = new MockStormApplication();
        mockStormApplication.execute(new HashMap<String,Object>(){
            {
                put("spoutNum",1234);
                put("loaded",true);
                put("mode", ApplicationEntity.Mode.CLUSTER);
            }
        },new StormEnvironment(ConfigFactory.load()));
        Assert.assertTrue(mockStormApplication.getAppConfig().isLoaded());
        Assert.assertEquals(1234,mockStormApplication.getAppConfig().getSpoutNum());
        Assert.assertEquals(ApplicationEntity.Mode.CLUSTER,mockStormApplication.getAppConfig().getMode());
    }

    @Test
    public void testGetConfigFromEnvironmentConfigFile(){
        MockStormApplication mockStormApplication = new MockStormApplication();
        mockStormApplication.execute(new StormEnvironment(ConfigFactory.load()));
        Assert.assertTrue(mockStormApplication.getAppConfig().isLoaded());
        Assert.assertEquals(3,mockStormApplication.getAppConfig().getSpoutNum());
        Assert.assertEquals(ApplicationEntity.Mode.LOCAL,mockStormApplication.getAppConfig().getMode());
    }

    @Test
    public void testRunApplicationWithSysConfig(){
        new MockStormApplication().run();
    }

    @Test
    public void testRunApplicationWithAppConfig() throws InterruptedException {
        MockStormApplication.MockStormConfiguration appConfig = new MockStormApplication.MockStormConfiguration();
        appConfig.setJarPath(DynamicJarPathFinder.findPath(MockStormApplication.class));
        appConfig.setSiteId("test_site");
        appConfig.setAppId("test_application_storm_topology");
        appConfig.setMode(ApplicationEntity.Mode.LOCAL);
        appConfig.setLoaded(true);
        appConfig.setSpoutNum(4);
        new MockStormApplication().run(appConfig);
    }
}