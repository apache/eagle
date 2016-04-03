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

package org.apache.eagle.security.crawler.audit;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import junit.framework.Assert;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.junit.Test;

import java.util.Map;

public class TestMetaDataAccessConfigRepo {

    @Test
    public void testStringToConfig() {
        String hdfsConfigStr = "web.fs.defaultFS: \"hdfs://sandbox.hortonworks.com:8020\"";
        Config config = ConfigFactory.parseString(hdfsConfigStr);
        Assert.assertTrue(config.hasPath(EagleConfigConstants.WEB_CONFIG));

        String hiveConfigStr = "web.accessType:\"metastoredb_jdbc\",web.password:\"hive\",web.user:\"hive\",web.jdbcDriverClassName:\"com.mysql.jdbc.Driver\",web.jdbcUrl:\"jdbc:mysql://sandbox.hortonworks.com/hive?createDatabaseIfNotExist=true\"";
        config = ConfigFactory.parseString(hiveConfigStr);
        Config hiveConfig = null;
        if(config.hasPath(EagleConfigConstants.WEB_CONFIG)) {
            hiveConfig = config.getConfig(EagleConfigConstants.WEB_CONFIG);
            Assert.assertTrue(hiveConfig.getString("accessType").equals("metastoredb_jdbc"));
        }

        String hbaseConfigStr = "web.hbase.zookeeper.property.clientPort: \"2181\", web.hbase.zookeeper.quorum: \"localhost\"";
        config = ConfigFactory.parseString(hbaseConfigStr);
        Config hbaseConfig = null;
        if(config.hasPath(EagleConfigConstants.WEB_CONFIG)) {
            hbaseConfig = config.getConfig(EagleConfigConstants.WEB_CONFIG);
            Assert.assertTrue(hbaseConfig.getString("hbase.zookeeper.property.clientPort").equals("2181"));
        }
    }
}
