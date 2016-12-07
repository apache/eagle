/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.apache.eagle.jpm.mr.running;


import backtype.storm.Config;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.FeederSpout;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.typesafe.config.ConfigFactory;
import junit.framework.Assert;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.eagle.jpm.mr.running.storm.MRRunningJobParseBolt;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourcefetch.model.AppInfo;
import org.apache.eagle.jpm.util.resourcefetch.model.AppsWrapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import static org.mockito.Mockito.*;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CuratorFrameworkFactory.class})
@PowerMockIgnore({"javax.*"})
public class TestMRRunningJobApplicationStorm {
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();
    private static TestingServer zk;
    private CuratorFramework curator;
    @Test
    public void testCleanUpWasInvoked() throws Exception {
        zk = new TestingServer();
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        daemonConf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
        mkClusterParam.setDaemonConf(daemonConf);

        com.typesafe.config.Config config = ConfigFactory.load();
        MRRunningJobConfig mrRunningJobConfig = MRRunningJobConfig.newInstance(config);
        mrRunningJobConfig.getZkStateConfig().zkQuorum = zk.getConnectString();
        List<String> conKeyKeys = makeConfKeyKeys(mrRunningJobConfig);

        MRRunningJobParseBolt mrRunningJobParseBolt = new MRRunningJobParseBolt(
            mrRunningJobConfig.getEagleServiceConfig(),
            mrRunningJobConfig.getEndpointConfig(),
            mrRunningJobConfig.getZkStateConfig(),
            conKeyKeys,
            config
        );

        curator = CuratorFrameworkFactory.newClient(zk.getConnectString(), new RetryOneTime(1));
        PowerMockito.mockStatic(CuratorFrameworkFactory.class);
        when(CuratorFrameworkFactory.newClient(anyString(), anyInt(), anyInt(), anyObject())).thenReturn(curator);

        String spoutName = "fakeMRRunningJobFetchSpout";
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, iLocalCluster -> {
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout(spoutName, new FeederSpout(new Fields("appId", "appInfo", "mrJobEntity")), 1).setNumTasks(1);
            builder.setBolt("mrRunningJobParseBolt", mrRunningJobParseBolt, 1).setNumTasks(1).fieldsGrouping(spoutName, new Fields("appId"));
            StormTopology topology = builder.createTopology();
            MockedSources mockedSources = new MockedSources();
            InputStream previousmrrunningapp = this.getClass().getResourceAsStream("/previousmrrunningapp.json");
            AppsWrapper appsWrapper = OBJ_MAPPER.readValue(previousmrrunningapp, AppsWrapper.class);
            List<AppInfo> appInfos = appsWrapper.getApps().getApp();
            mockedSources.addMockData(spoutName, new Values(appInfos.get(0).getId(), appInfos.get(0), null));

            CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
            completeTopologyParam.setMockedSources(mockedSources);
            Config conf = new Config();
            conf.setMessageTimeoutSecs(20);
            conf.setNumWorkers(2);
            completeTopologyParam.setStormConf(conf);
            Testing.completeTopology(iLocalCluster, topology, completeTopologyParam);

            Assert.assertEquals(curator.getState(), CuratorFrameworkState.STOPPED);
        } );
    }

    private List<String> makeConfKeyKeys(MRRunningJobConfig mrRunningJobConfig) {
        String[] confKeyPatternsSplit = mrRunningJobConfig.getConfig().getString("MRConfigureKeys.jobConfigKey").split(",");
        List<String> confKeyKeys = new ArrayList<>(confKeyPatternsSplit.length);
        for (String confKeyPattern : confKeyPatternsSplit) {
            confKeyKeys.add(confKeyPattern.trim());
        }
        confKeyKeys.add(Constants.JobConfiguration.CASCADING_JOB);
        confKeyKeys.add(Constants.JobConfiguration.HIVE_JOB);
        confKeyKeys.add(Constants.JobConfiguration.PIG_JOB);
        confKeyKeys.add(Constants.JobConfiguration.SCOOBI_JOB);
        confKeyKeys.add(0, mrRunningJobConfig.getConfig().getString("MRConfigureKeys.jobNameKey"));
        return confKeyKeys;
    }
}
