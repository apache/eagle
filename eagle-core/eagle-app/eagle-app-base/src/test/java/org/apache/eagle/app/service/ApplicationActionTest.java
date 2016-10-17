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
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

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
}