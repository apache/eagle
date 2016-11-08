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

package org.apache.eagle.alert.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.FileUtils;
import org.apache.eagle.alert.utils.ConfigUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class ConfigUtilsTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testToProperties() throws IOException {
        Config config = ConfigFactory.parseFile(genConfig());
        Properties properties = ConfigUtils.toProperties(config);
        System.out.print(properties);
        Assert.assertEquals("{metric={sink={stdout={}, elasticsearch={hosts=[localhost:9200], index=alert_metric_test}, kafka={topic=alert_metric_test, bootstrap.servers=localhost:9092}, logger={level=INFO}}}, zkConfig={zkQuorum=localhost:2181, zkRoot=/alert}}", properties.toString());
    }

    private File genConfig() throws IOException {
        File file = tempFolder.newFile("application-config.conf");
        String fileContent = "{\n" +
                "  metric {\n" +
                "    sink {\n" +
                "      stdout {\n" +
                "        // console metric sink\n" +
                "      }\n" +
                "      kafka {\n" +
                "        \"topic\": \"alert_metric_test\"\n" +
                "        \"bootstrap.servers\": \"localhost:9092\"\n" +
                "      }\n" +
                "      logger {\n" +
                "        level = \"INFO\"\n" +
                "      }\n" +
                "      elasticsearch {\n" +
                "        hosts = [\"localhost:9200\"]\n" +
                "        index = \"alert_metric_test\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "  zkConfig {\n" +
                "    \"zkQuorum\": \"localhost:2181\"\n" +
                "    \"zkRoot\": \"/alert\"\n" +
                "  }\n" +
                "}";
        FileUtils.writeStringToFile(file, fileContent);
        return file;
    }
}
