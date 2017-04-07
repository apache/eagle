/*
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
package org.apache.eagle.jpm.mr.history.storm;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import com.google.common.collect.Lists;
import com.sun.jersey.api.client.Client;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.eagle.jpm.mr.history.MRHistoryJobConfig;
import org.apache.eagle.jpm.mr.history.crawler.JHFCrawlerDriver;
import org.apache.eagle.jpm.mr.history.crawler.JobHistoryContentFilter;
import org.apache.eagle.jpm.mr.history.crawler.JobHistoryContentFilterBuilder;
import org.apache.eagle.jpm.mr.history.metrics.JobCountMetricsGenerator;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.HDFSUtil;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import static org.mockito.Mockito.*;

/**
 * Created by luokun on 12/1/16.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest( {CuratorFrameworkFactory.class, HDFSUtil.class, JobCountMetricsGenerator.class, JobHistorySpout.class})
@PowerMockIgnore( {"javax.*", "com.sun.org.*", "org.apache.hadoop.conf.*"})
public class JobHistorySpoutTest {

    private static final Logger LOG = LoggerFactory.getLogger(JobHistorySpoutTest.class);

    private TestingServer server;
    private CuratorFramework zookeeper;
    private FileSystem hdfs;
    private MRHistoryJobConfig appConfig;
    private EagleServiceClientImpl client;

    @Before
    public void setUp() throws Exception {
        this.appConfig = MRHistoryJobConfig.newInstance(ConfigFactory.load());
        createZk();
        mockHdfs();
        setupMock();
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (zookeeper != null) {
                if (!zookeeper.getState().equals(CuratorFrameworkState.STOPPED)) {
                    zookeeper.close();
                }
            }
        } finally {
            if (server != null) {
                server.close();
            }
        }
    }

    @Test
    public void testSpout() throws Exception {
        JobHistorySpout spout = createJobHistorySpout();
        List<Object> tuples = new ArrayList<>();
        SpoutOutputCollector collector = new SpoutOutputCollector(new ISpoutOutputCollector() {
            @Override
            public List<Integer> emit(String s, List<Object> list, Object o) {
                tuples.add(list);
                return null;
            }

            @Override
            public void emitDirect(int i, String s, List<Object> list, Object o) {

            }

            @Override
            public void reportError(Throwable throwable) {

            }
        });
        PowerMockito.mockStatic(CuratorFrameworkFactory.class);
        when(CuratorFrameworkFactory.newClient(anyString(), anyInt(), anyInt(), any(RetryNTimes.class))).thenReturn(zookeeper);
        spout.open(null, createTopologyContext(), collector);
        Field driverField = JobHistorySpout.class.getDeclaredField("driver");
        driverField.setAccessible(true);
        JHFCrawlerDriver driver = (JHFCrawlerDriver) driverField.get(spout);
        Assert.assertNotNull(driver);
        spout.nextTuple();
        Assert.assertTrue(zookeeper.checkExists().forPath("/apps/mr/history/sandbox/partitions/0/timeStamps") != null);
        Assert.assertTrue(StringUtils.isNotEmpty((new String(zookeeper.getData().forPath("/apps/mr/history/sandbox/partitions/0/timeStamps"), "UTF-8"))));
        verify(client, times(2)).create(any());

    }

    private void mockHdfs() throws Exception {
        PowerMockito.mockStatic(HDFSUtil.class);
        hdfs = mock(FileSystem.class);
        when(HDFSUtil.getFileSystem(any(Configuration.class))).thenReturn(hdfs);
        FileStatus fileDirStatus = new FileStatus(100l, true, 3, 1000l, new Date().getTime(), new Path("/user/history/done/2016/12/09/000508"));
        when(hdfs.listStatus(any(Path.class))).thenReturn(new FileStatus[] {fileDirStatus});
        FileStatus filePartitionStatus = new FileStatus(100l, false, 3, 1000l, new Date().getTime(), new Path("/user/history/done/2016/12/09/000508/job_1479206441898_508949-1481299030929-testhistory.jhist"));
        when(hdfs.listStatus(any(Path.class), any(PathFilter.class))).thenReturn(new FileStatus[] {filePartitionStatus});
        Path historyFilePath = mock(Path.class);
        Path historyConfPath = mock(Path.class);
        PowerMockito.whenNew(Path.class).withArguments("/mr-history/done/2017/04/07/000508/job_1479206441898_508949-1481299030929-testhistory.jhist").thenReturn(historyFilePath);
        PowerMockito.whenNew(Path.class).withArguments("/mr-history/done/2017/04/07/000508/job_1479206441898_508949_conf.xml").thenReturn(historyConfPath);

        when((InputStream) hdfs.open(historyFilePath)).thenReturn(this.getClass().getResourceAsStream("job_1479206441898_508949-1481299030929-testhistory.jhist"));
        when((InputStream) hdfs.open(historyConfPath)).thenReturn(this.getClass().getResourceAsStream("job_1479206441898_508949_conf.xml"));
    }

    private TopologyContext createTopologyContext() {
        TopologyContext topologyContext = mock(TopologyContext.class);
        when(topologyContext.getThisTaskId()).thenReturn(1);
        when(topologyContext.getComponentId(1)).thenReturn("test_component");
        List<Integer> globalTaskIds = Lists.newArrayList(1);
        when(topologyContext.getComponentTasks("test_component")).thenReturn(globalTaskIds);
        return topologyContext;
    }

    private void setupMock() throws Exception {
        client = mock(EagleServiceClientImpl.class);
        MRHistoryJobConfig.EagleServiceConfig eagleServiceConfig = appConfig.getEagleServiceConfig();
        PowerMockito.whenNew(EagleServiceClientImpl.class).withArguments(
            eagleServiceConfig.eagleServiceHost,
            eagleServiceConfig.eagleServicePort,
            eagleServiceConfig.username,
            eagleServiceConfig.password).thenReturn(client);
        PowerMockito.whenNew(EagleServiceClientImpl.class).withAnyArguments().thenReturn(client);
        when(client.create(any())).thenReturn(null);
        when(client.getJerseyClient()).thenReturn(new Client());
    }

    private JobHistorySpout createJobHistorySpout() {
        //1. trigger init conf
        com.typesafe.config.Config jhfAppConf = appConfig.getConfig();

        //2. init JobHistoryContentFilter
        final JobHistoryContentFilterBuilder builder = JobHistoryContentFilterBuilder.newBuilder().acceptJobFile().acceptJobConfFile();
        String[] confKeyPatternsSplit = jhfAppConf.getString("MRConfigureKeys.jobConfigKey").split(",");
        List<String> confKeyPatterns = new ArrayList<>(confKeyPatternsSplit.length);
        for (String confKeyPattern : confKeyPatternsSplit) {
            confKeyPatterns.add(confKeyPattern.trim());
        }
        confKeyPatterns.add(Constants.JobConfiguration.CASCADING_JOB);
        confKeyPatterns.add(Constants.JobConfiguration.HIVE_JOB);
        confKeyPatterns.add(Constants.JobConfiguration.PIG_JOB);
        confKeyPatterns.add(Constants.JobConfiguration.SCOOBI_JOB);

        String jobNameKey = jhfAppConf.getString("MRConfigureKeys.jobNameKey");
        builder.setJobNameKey(jobNameKey);

        for (String key : confKeyPatterns) {
            builder.includeJobKeyPatterns(Pattern.compile(key));
        }
        JobHistoryContentFilter filter = builder.build();
        return new JobHistorySpout(filter, appConfig);
    }

    private void createZk() throws Exception {
        int port = 2111;
        File logFile = new File(System.getProperty("java.io.tmpdir"), "zk/logs/zookeeper-test-" + port);
        FileUtils.deleteQuietly(logFile);
        server = new TestingServer(port, logFile);
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zookeeper = CuratorFrameworkFactory.newClient(server.getConnectString(), retryPolicy);
    }

}