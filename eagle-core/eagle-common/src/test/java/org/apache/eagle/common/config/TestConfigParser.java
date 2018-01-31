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
package org.apache.eagle.common.config;

import com.typesafe.config.Config;
import org.junit.Assert;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

/**
 * @Since 11/22/16.
 */
public class TestConfigParser {

    @Test
    public void testLoadNullArgs() throws ParseException {
        System.setProperty("config.resource", "application-test.conf");
        ConfigOptionParser configOptionParser = new ConfigOptionParser();
        Config config = configOptionParser.load(null);
        Assert.assertEquals("UTC", config.getString("service.timezone"));
    }
}
