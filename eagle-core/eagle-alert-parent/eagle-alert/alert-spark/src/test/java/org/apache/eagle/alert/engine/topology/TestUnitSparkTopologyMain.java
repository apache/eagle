/**
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
package org.apache.eagle.alert.engine.topology;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.engine.runner.UnitSparkTopologyRunner;
import org.junit.Ignore;
import org.junit.Test;

public class TestUnitSparkTopologyMain {

    @Ignore
    @Test
    public void testTopologyRun() throws InterruptedException {
        testTopologyRun("/spark/application-spark.conf");
    }

    public void testTopologyRun(String configResourceName) throws InterruptedException {
        ConfigFactory.invalidateCaches();
        System.setProperty("config.resource", configResourceName);
        System.out.print("Set config.resource = " + configResourceName);
        Config config = ConfigFactory.load();
        new UnitSparkTopologyRunner(config).run();
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length > 0) {
            new TestUnitSparkTopologyMain().testTopologyRun(args[0]);
        } else {
            new TestUnitSparkTopologyMain().testTopologyRun();
        }
    }
}
