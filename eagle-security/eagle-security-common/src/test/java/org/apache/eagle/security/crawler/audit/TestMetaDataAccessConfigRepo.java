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


import com.typesafe.config.*;
import junit.framework.Assert;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.junit.Test;

import java.util.Map;

public class TestMetaDataAccessConfigRepo {

    @Test
    public void testStringToConfig() {
        String hdfsConfigStr = "classification.fs.defaultFS=hdfs://sandbox.hortonworks.com:8020";
        ConfigParseOptions options = ConfigParseOptions.defaults()
                .setSyntax(ConfigSyntax.PROPERTIES)
                .setAllowMissing(false);
        Config config = ConfigFactory.parseString(hdfsConfigStr, options);
        Assert.assertTrue(config.hasPath(EagleConfigConstants.CLASSIFICATION_CONFIG));

        String hiveConfigStr = "classification.accessType=metastoredb_jdbc\nclassification.password=hive\nclassification.user=hive\nclassification.jdbcDriverClassName=com.mysql.jdbc.Driver\nclassification.jdbcUrl=jdbc:mysql://sandbox.hortonworks.com/hive?createDatabaseIfNotExist=true";
        config = ConfigFactory.parseString(hiveConfigStr, options);
        Config hiveConfig = null;
        if(config.hasPath(EagleConfigConstants.CLASSIFICATION_CONFIG)) {
            hiveConfig = config.getConfig(EagleConfigConstants.CLASSIFICATION_CONFIG);
            Assert.assertTrue(hiveConfig.getString("accessType").equals("metastoredb_jdbc"));
        }

        String hbaseConfigStr = "classification.hbase.zookeeper.property.clientPort=2181\nclassification.hbase.zookeeper.quorum=localhost";
        config = ConfigFactory.parseString(hbaseConfigStr, options);
        Config hbaseConfig = null;
        if(config.hasPath(EagleConfigConstants.CLASSIFICATION_CONFIG)) {
            hbaseConfig = config.getConfig(EagleConfigConstants.CLASSIFICATION_CONFIG);
            Assert.assertTrue(hbaseConfig.getString("hbase.zookeeper.property.clientPort").equals("2181"));
        }

        String appConfigStr = "classification.hbase.zookeeper.property.clientPort=2181\nclassification.hbase.zookeeper.quorum=sandbox.hortonworks.com\n\napp.envContextConfig.env=storm\napp.envContextConfig.mode=cluster\napp.dataSourceConfig.topic=sandbox_hbase_security_log\napp.dataSourceConfig.zkConnection=127.0.0.1:2181\napp.dataSourceConfig.zkConnectionTimeoutMS=15000\napp.dataSourceConfig.brokerZkPath=/brokers\napp.dataSourceConfig.fetchSize=1048586\napp.dataSourceConfig.transactionZKServers=127.0.0.1\napp.dataSourceConfig.transactionZKPort=2181\napp.dataSourceConfig.transactionZKRoot=/consumers\napp.dataSourceConfig.consumerGroupId=eagle.hbasesecurity.consumer\napp.dataSourceConfig.transactionStateUpdateMS=2000\napp.dataSourceConfig.deserializerClass=org.apache.eagle.security.hbase.parse.HbaseAuditLogKafkaDeserializer\napp.eagleProps.site=sandbox\napp.eagleProps.application=hbaseSecurityLog\napp.eagleProps.dataJoinPollIntervalSec=30\napp.eagleProps.mailHost=mailHost.com\napp.eagleProps.mailSmtpPort=25\napp.eagleProps.mailDebug=true\napp.eagleProps.eagleService.host=localhost\napp.eagleProps.eagleService.port=9099\napp.eagleProps.eagleService.username=admin\napp.eagleProps.eagleService.password=secret";
        config = ConfigFactory.parseString(appConfigStr, options);
        Config appConfig = null;
        if(config.hasPath(EagleConfigConstants.APP_CONFIG)) {
            appConfig = config.getConfig(EagleConfigConstants.APP_CONFIG);
            Assert.assertTrue(appConfig.getString("envContextConfig.mode").equals("cluster"));
        }
    }
}
