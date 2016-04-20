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

package org.apache.eagle.security.hbase;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.junit.Test;


public class TestHbaseAuditLogProcessTopology {
    @Test
    public void test() throws Exception {
        //Config baseConfig = ConfigFactory.load("eagle-scheduler.conf");
        ConfigParseOptions options = ConfigParseOptions.defaults()
                .setSyntax(ConfigSyntax.PROPERTIES)
                .setAllowMissing(false);
        String topoConfigStr = "web.hbase.zookeeper.property.clientPort=2181\nweb.hbase.zookeeper.quorum=sandbox.hortonworks.com\n\napp.envContextConfig.env=storm\napp.envContextConfig.mode=local\napp.dataSourceConfig.topic=sandbox_hbase_security_log\napp.dataSourceConfig.zkConnection=sandbox.hortonworks.com:2181\napp.dataSourceConfig.zkConnectionTimeoutMS=15000\napp.dataSourceConfig.brokerZkPath=/brokers\napp.dataSourceConfig.fetchSize=1048586\napp.dataSourceConfig.transactionZKServers=sandbox.hortonworks.com\napp.dataSourceConfig.transactionZKPort=2181\napp.dataSourceConfig.transactionZKRoot=/consumers\napp.dataSourceConfig.consumerGroupId=eagle.hbasesecurity.consumer\napp.dataSourceConfig.transactionStateUpdateMS=2000\napp.dataSourceConfig.deserializerClass=org.apache.eagle.security.hbase.parse.HbaseAuditLogKafkaDeserializer\napp.eagleProps.site=sandbox\napp.eagleProps.application=hbaseSecurityLog\napp.eagleProps.dataJoinPollIntervalSec=30\napp.eagleProps.mailHost=mailHost.com\napp.eagleProps.mailSmtpPort=25\napp.eagleProps.mailDebug=true\napp.eagleProps.eagleService.host=localhost\napp.eagleProps.eagleService.port=9098\napp.eagleProps.eagleService.username=admin\napp.eagleProps.eagleService.password=secret";

        Config topoConfig = ConfigFactory.parseString(topoConfigStr, options);
        Config conf = topoConfig.getConfig(EagleConfigConstants.APP_CONFIG);

        HbaseAuditLogMonitoringTopology topology = new HbaseAuditLogMonitoringTopology();
        //topology.submit("", conf);
    }
}
