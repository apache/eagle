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

package org.apache.eagle.jpm.mr.running;

import com.typesafe.config.ConfigFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.eagle.jpm.mr.running.recover.MRRunningJobManager;
import org.apache.eagle.jpm.util.jobrecover.RunningJobManager;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;


@RunWith(PowerMockRunner.class)
@PrepareForTest({MRRunningJobManager.class, RunningJobManager.class, LoggerFactory.class})
@PowerMockIgnore({"javax.*"})
public class MRRunningJobManagerTest {
    private static TestingServer zk;
    private static com.typesafe.config.Config config = ConfigFactory.load();
    private static CuratorFramework curator;
    private static final String SHARE_RESOURCES = "/apps/mr/running/sandbox/yarnAppId/jobId";
    private static final int QTY = 5;
    private static final int REPETITIONS = QTY * 10;
    private static MRRunningJobConfig.EndpointConfig endpointConfig;
    private static MRRunningJobConfig.ZKStateConfig zkStateConfig;
    private static org.slf4j.Logger log = mock(org.slf4j.Logger.class);

    @BeforeClass
    public static void setupZookeeper() throws Exception {
        zk = new TestingServer();
        curator = CuratorFrameworkFactory.newClient(zk.getConnectString(), new ExponentialBackoffRetry(1000, 3));
        curator.start();
        curator.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(SHARE_RESOURCES);
        MRRunningJobConfig mrRunningJobConfig = MRRunningJobConfig.newInstance(config);
        zkStateConfig = mrRunningJobConfig.getZkStateConfig();
        zkStateConfig.zkQuorum = zk.getConnectString();
        endpointConfig = mrRunningJobConfig.getEndpointConfig();
        mockStatic(LoggerFactory.class);
        when(LoggerFactory.getLogger(any(Class.class))).thenReturn(log);
    }

    @AfterClass
    public static void teardownZookeeper() throws Exception {
        if (curator.checkExists().forPath(SHARE_RESOURCES) != null) {
            curator.delete().deletingChildrenIfNeeded().forPath(SHARE_RESOURCES);
        }
        CloseableUtils.closeQuietly(curator);
        CloseableUtils.closeQuietly(zk);
    }

    @Test
    public void testMRRunningJobManagerDelWithLock() throws Exception {
        Assert.assertTrue(curator.checkExists().forPath(SHARE_RESOURCES) != null);

        ExecutorService service = Executors.newFixedThreadPool(QTY);
        for (int i = 0; i < QTY; ++i) {
            Callable<Void> task = () -> {
                try {
                    MRRunningJobManager mrRunningJobManager = new MRRunningJobManager(zkStateConfig, endpointConfig.site);
                    for (int j = 0; j < REPETITIONS; ++j) {
                        mrRunningJobManager.delete("yarnAppId", "jobId");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    // log or do something
                }
                return null;
            };
            service.submit(task);
        }

        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);
        Assert.assertTrue(curator.checkExists().forPath(SHARE_RESOURCES) == null);
        verify(log, never()).error(anyString(), anyString(), anyString(), anyString(), any(Throwable.class));
        verify(log, never()).error(anyString(), anyString(), anyString());
        verify(log, never()).error(anyString(), any(Throwable.class));

    }

}
