/**
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
package org.apache.eagle.datastream;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.datastream.storm.StormExecutionEnvironment;
import org.junit.Assert;
import org.junit.Test;

/**
 * @since 12/5/15
 */
public class TestExecutionEnvironmentJava {

    @Test
    public void testGetEnvInJava() {
        StormExecutionEnvironment env0 = ExecutionEnvironments.get(StormExecutionEnvironment.class);
        Assert.assertNotNull(env0);

        StormExecutionEnvironment env1 = ExecutionEnvironments.get(new String[]{}, StormExecutionEnvironment.class);
        Assert.assertNotNull(env1);
        Config config = ConfigFactory.load();
        StormExecutionEnvironment env2 = ExecutionEnvironments.get(config, StormExecutionEnvironment.class);
        Assert.assertNotNull(env2);
    }
}