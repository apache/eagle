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
package org.apache.eagle.security.hive.jobrunning;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import com.typesafe.config.ConfigFactory;
import junit.framework.Assert;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.junit.*;
import static org.mockito.Mockito.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @Since 12/5/16.
 */
public class TestHiveJobFetchSpout {

    private static TestingServer zk;
    private static com.typesafe.config.Config config;
    private static CuratorFramework curator;
    private static final String SHARE_RESOURCES = "/apps/hive/running/sanbox/jobs/0/lastFinishTime";

    @BeforeClass
    public static void setupZookeeper() throws Exception {
        zk = new TestingServer();
        curator = CuratorFrameworkFactory.newClient(zk.getConnectString(), new ExponentialBackoffRetry(1000, 3));
        curator.start();
        curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(SHARE_RESOURCES);
        config = ConfigFactory.parseMap(new HashMap<String, Object>() {{
            put("dataSourceConfig.RMEndPoints", "http://server.eagle.apache.org:8088");
            put("dataSourceConfig.HSEndPoint", "http://server.eagle.apache.org:19888");
            put("dataSourceConfig.zkQuorum", zk.getConnectString());
            put("dataSourceConfig.zkRoot", "/apps/hive/running");
            put("dataSourceConfig.zkSessionTimeoutMs", 15000);
            put("dataSourceConfig.zkRetryTimes", 3);
            put("dataSourceConfig.zkRetryInterval", 2000);
            put("dataSourceConfig.partitionerCls", "org.apache.eagle.job.DefaultJobPartitionerImpl");
            put("siteId", "sanbox");
        }});
    }

    @AfterClass
    public static void teardownZookeeper() throws Exception {
        if(curator.checkExists().forPath(SHARE_RESOURCES) != null) {
            curator.delete().deletingChildrenIfNeeded().forPath(SHARE_RESOURCES);
        }
        CloseableUtils.closeQuietly(curator);
        CloseableUtils.closeQuietly(zk);
    }

    @Before
    public void setDefaultValues() throws Exception {
        curator.setData().forPath(SHARE_RESOURCES, String.valueOf(0).getBytes());
    }

    @Test
    public void testOpen() throws Exception {
        Map conf = mock(HashMap.class);
        TopologyContext context = mock(TopologyContext.class);
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        when(context.getThisTaskId()).thenReturn(1);
        when(context.getComponentTasks(anyString())).thenReturn(new ArrayList<Integer>() {{
            add(1);
        }});
        HiveJobRunningSourcedStormSpoutProvider provider = new HiveJobRunningSourcedStormSpoutProvider();
        HiveJobFetchSpout spout = (HiveJobFetchSpout)provider.getSpout(config, 1);
        spout.open(conf, context, collector);
        Long yesterday = Long.valueOf(new String(curator.getData().forPath(SHARE_RESOURCES)));
        Assert.assertTrue(System.currentTimeMillis() > yesterday);
    }
}
