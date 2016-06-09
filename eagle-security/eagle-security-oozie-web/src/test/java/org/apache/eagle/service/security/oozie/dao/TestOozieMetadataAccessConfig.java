/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.service.security.oozie.dao;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.junit.Assert;
import org.junit.Test;

public class TestOozieMetadataAccessConfig {
    @Test
    public void testGetOozieConfig() throws Exception {
        String oozieConfigStr = "classification.accessType=oozie_api\nclassification.oozieUrl=http://localhost:11000/oozie\nclassification.filter=status=RUNNING\nclassification.authType=SIMPLE";
        ConfigParseOptions options = ConfigParseOptions.defaults()
                .setSyntax(ConfigSyntax.PROPERTIES)
                .setAllowMissing(false);
        Config config = ConfigFactory.parseString(oozieConfigStr, options);
        config = config.getConfig(EagleConfigConstants.CLASSIFICATION_CONFIG);
        OozieMetadataAccessConfig oozieMetadataAccessConfig = OozieMetadataAccessConfig.config2Entity(config);
        System.out.print(oozieMetadataAccessConfig);
        Assert.assertEquals("oozie_api", oozieMetadataAccessConfig.getAccessType());
        Assert.assertEquals("http://localhost:11000/oozie", oozieMetadataAccessConfig.getOozieUrl());
        Assert.assertEquals("status=RUNNING", oozieMetadataAccessConfig.getFilter());
        Assert.assertEquals("SIMPLE", oozieMetadataAccessConfig.getAuthType());
    }
}
