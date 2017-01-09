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

package org.apache.eagle.jpm.mr.running.parser;

import com.sun.jersey.api.client.Client;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.eagle.jpm.mr.running.MRRunningJobConfig;
import org.apache.eagle.jpm.mr.running.parser.metrics.JobExecutionMetricsCreationListener;
import org.apache.eagle.jpm.mr.running.recover.MRRunningJobManager;
import org.apache.eagle.jpm.mr.runningentity.JobConfig;
import org.apache.eagle.jpm.mr.runningentity.JobExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourcefetch.RMResourceFetcher;
import org.apache.eagle.jpm.util.resourcefetch.connection.InputStreamUtils;
import org.apache.eagle.jpm.util.resourcefetch.connection.URLConnectionUtils;
import org.apache.eagle.jpm.util.resourcefetch.model.AppInfo;
import org.apache.eagle.jpm.util.resourcefetch.model.AppsWrapper;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.*;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({InputStreamUtils.class, MRJobParser.class, URLConnectionUtils.class, Math.class, MRJobEntityCreationHandler.class})
@PowerMockIgnore({"javax.*", "org.w3c.*", "com.sun.org.apache.xerces.*","org.apache.xerces.*"})
public class MRJobParserTest {
    private static final String ZK_JOB_PATH = "/apps/mr/running/sandbox/jobs/application_1479206441898_30784/job_1479206441898_30784";
    private static final String ZK_APP_PATH = "/apps/mr/running/sandbox/jobs/application_1479206441898_30784";
    private static final String JOB_CONF_URL = "http://host.domain.com:8088/proxy/application_1479206441898_30784/ws/v1/mapreduce/jobs/job_1479206441898_30784/conf?anonymous=true";
    private static final String JOB_COUNT_URL = "http://host.domain.com:8088/proxy/application_1479206441898_30784/ws/v1/mapreduce/jobs/job_1479206441898_30784/counters?anonymous=true";
    private static final String JOB_ID = "job_1479206441898_30784";
    private static final String JOB_URL = "http://host.domain.com:8088/proxy/application_1479206441898_30784/ws/v1/mapreduce/jobs?anonymous=true";
    private static final String DATA_FROM_ZK = "{\"entityTags\":\"{\\\"jobName\\\":\\\"oozie:launcher:T=shell:W=wf_co_xxx_xxx_v3:A=extract_org_data:ID=0002383-161115184801730-oozie-oozi-W\\\",\\\"jobId\\\":\\\"job_1479206441898_30784\\\",\\\"site\\\":\\\"sandbox\\\",\\\"jobDefId\\\":\\\"eagletest\\\",\\\"jobType\\\":\\\"HIVE\\\",\\\"user\\\":\\\"xxx\\\",\\\"queue\\\":\\\"xxx\\\"}\",\"appInfo\":\"{\\\"applicationType\\\":\\\"MAPREDUCE\\\",\\\"startedTime\\\":\\\"1479328221694\\\",\\\"finalStatus\\\":\\\"UNDEFINED\\\",\\\"trackingUrl\\\":\\\"http:\\\\\\/\\\\\\/host.domain.com:8088\\\\\\/proxy\\\\\\/application_1479206441898_30784\\\\\\/\\\",\\\"runningContainers\\\":\\\"2\\\",\\\"trackingUI\\\":\\\"ApplicationMaster\\\",\\\"clusterId\\\":\\\"1479206441898\\\",\\\"amContainerLogs\\\":\\\"http:\\\\\\/\\\\\\/host.domain.com:8088\\\\\\/node\\\\\\/containerlogs\\\\\\/container_e11_1479206441898_30784_01_000001\\\\\\/xxx\\\",\\\"allocatedVCores\\\":\\\"2\\\",\\\"diagnostics\\\":\\\"\\\",\\\"name\\\":\\\"oozie:launcher:T=shell:W=wf_co_xxx_xxx_v3:A=extract_org_data:ID=0002383-161115184801730-oozie-oozi-W\\\",\\\"progress\\\":\\\"95.0\\\",\\\"finishedTime\\\":\\\"0\\\",\\\"allocatedMB\\\":\\\"3072\\\",\\\"id\\\":\\\"application_1479206441898_30784\\\",\\\"state\\\":\\\"RUNNING\\\",\\\"amHostHttpAddress\\\":\\\"host.domain.com:8088\\\",\\\"user\\\":\\\"xxx\\\",\\\"queue\\\":\\\"xxx\\\",\\\"elapsedTime\\\":\\\"13367402\\\"}\"}";
    private static TestingServer zk;
    private static String ZKROOT;
    private static MRRunningJobConfig mrRunningJobConfig;
    private static Config config = ConfigFactory.load();
    private static CuratorFramework curator;
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();
    private EagleServiceClientImpl client;

    @BeforeClass
    public static void startZookeeper() throws Exception {
        zk = new TestingServer();
        curator = CuratorFrameworkFactory.newClient(zk.getConnectString(), new RetryOneTime(1));
        mrRunningJobConfig = MRRunningJobConfig.newInstance(config);
        mrRunningJobConfig.getZkStateConfig().zkQuorum = zk.getConnectString();
        ZKROOT = mrRunningJobConfig.getZkStateConfig().zkRoot;
        OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
        curator.start();
    }

    @AfterClass
    public static void teardownZookeeper() throws IOException {
        curator.close();
        zk.stop();
    }

    @Before
    public void cleanZkPath() throws Exception {
        if (curator.checkExists().forPath(ZK_JOB_PATH) != null) {
            curator.delete().deletingChildrenIfNeeded().forPath(ZK_JOB_PATH);
        }
        if (curator.checkExists().forPath(ZK_APP_PATH) != null) {
            curator.delete().deletingChildrenIfNeeded().forPath(ZK_APP_PATH);
        }
        if (curator.checkExists().forPath(ZKROOT) != null) {
            curator.delete().deletingChildrenIfNeeded().forPath(ZKROOT);
        }
    }

    @Test
    public void testMRJobParser() throws Exception {
        //TODO fetch task attempt when(Math.random()).thenReturn(0.0); http://host.domain.com:8088/proxy/application_1479206441898_30784/ws/v1/mapreduce/jobs/job_1479206441898_30784/tasks?anonymous=true
        setupMock();

        mockInputJobSteam("/mrjob_30784.json", JOB_URL);
        mockInputJobSteam("/jobcounts_30784.json", JOB_COUNT_URL);
        mockGetConnection("/mrconf_30784.xml");


        Assert.assertTrue(curator.checkExists().forPath(ZKROOT) == null);
        Assert.assertTrue(curator.checkExists().forPath(ZK_JOB_PATH) == null);
        List<String> confKeyKeys = makeConfKeyKeys(mrRunningJobConfig);

        InputStream previousmrrunningapp = this.getClass().getResourceAsStream("/previousmrrunningapp.json");
        AppsWrapper appsWrapper = OBJ_MAPPER.readValue(previousmrrunningapp, AppsWrapper.class);
        List<AppInfo> appInfos = appsWrapper.getApps().getApp();
        AppInfo app1 = appInfos.get(0);
        Map<String, JobExecutionAPIEntity> mrJobs = null;

        MRRunningJobManager runningJobManager = new MRRunningJobManager(mrRunningJobConfig.getZkStateConfig());
        RMResourceFetcher resourceFetcher = new RMResourceFetcher(mrRunningJobConfig.getEndpointConfig().rmUrls);
        MRJobParser mrJobParser = new MRJobParser(mrRunningJobConfig.getEndpointConfig(), mrRunningJobConfig.getEagleServiceConfig(),
                app1, mrJobs, runningJobManager, resourceFetcher, confKeyKeys, config);


        Map<String, JobExecutionAPIEntity> jobIdToJobExecutionAPIEntity = getMrJobs(mrJobParser);
        Map<String, JobConfig> jobIdToJobConfig = getMrJobConfigs(mrJobParser);
        MRJobEntityCreationHandler mrJobEntityCreationHandler = getMrJobEntityCreationHandler(mrJobParser);
        List<TaggedLogAPIEntity> entities = getMrJobEntityCreationHandlerEntities(mrJobEntityCreationHandler);
        Assert.assertTrue(jobIdToJobExecutionAPIEntity.isEmpty());
        Assert.assertTrue(jobIdToJobConfig.isEmpty());
        Assert.assertTrue(entities.isEmpty());
        Assert.assertTrue(mrJobParser.status() == MRJobParser.ParserStatus.FINISHED);

        mrJobParser.run();

        Assert.assertTrue(jobIdToJobExecutionAPIEntity.size() == 1);

        JobExecutionAPIEntity jobExecutionAPIEntity = jobIdToJobExecutionAPIEntity.get(JOB_ID);
        Assert.assertEquals("AppInfo{id='application_1479206441898_30784', user='xxx', name='oozie:launcher:T=shell:W=wf_co_xxx_xxx_v3:A=extract_org_data:ID=0002383-161115184801730-oozie-oozi-W', queue='xxx', state='RUNNING', finalStatus='UNDEFINED', progress=95.0, trackingUI='ApplicationMaster', trackingUrl='http://host.domain.com:8088/proxy/application_1479206441898_30784/', diagnostics='', clusterId='1479206441898', applicationType='MAPREDUCE', startedTime=1479328221694, finishedTime=0, elapsedTime=13367402, amContainerLogs='http://host.domain.com:8088/node/containerlogs/container_e11_1479206441898_30784_01_000001/xxx', amHostHttpAddress='host.domain.com:8088', allocatedMB=3072, allocatedVCores=2, runningContainers=2}", jobExecutionAPIEntity.getAppInfo().toString());
        Assert.assertEquals("RUNNING", jobExecutionAPIEntity.getCurrentState());
        Assert.assertEquals("RUNNING", jobExecutionAPIEntity.getInternalState());
        Assert.assertEquals("prefix:null, timestamp:1479328221694, humanReadableDate:2016-11-16 20:30:21,694, tags: jobName=oozie:launcher:T=shell:W=wf_co_xxx_xxx_v3:A=extract_org_data:ID=0002383-161115184801730-oozie-oozi-W,jobId=job_1479206441898_30784,site=sandbox,jobDefId=eagletest,jobType=HIVE,user=xxx,queue=xxx,, encodedRowkey:null", jobExecutionAPIEntity.toString());
        //Assert.assertEquals("prefix:null, timestamp:1479328221694, humanReadableDate:2016-11-16 20:30:21,694, tags: jobName=oozie:launcher:T=shell:W=wf_co_xxx_xxx_v3:A=extract_org_data:ID=0002383-161115184801730-oozie-oozi-W,jobId=job_1479206441898_30784,site=sandbox,jobDefId=oozie:launcher-shell-wf_co_xxx_xxx_v3-extract_org_data~,jobType=HIVE,user=xxx,queue=xxx,, encodedRowkey:null", jobExecutionAPIEntity.toString());
        Assert.assertTrue(mrJobParser.status() == MRJobParser.ParserStatus.FINISHED);
        Assert.assertEquals("{eagle.job.name=eagletest, hive.optimize.skewjoin.compiletime=false, hive.query.string=insert overwrite table xxxx}", jobExecutionAPIEntity.getJobConfig().toString());
        Assert.assertTrue(jobIdToJobConfig.size() == 1);
        Assert.assertEquals("{eagle.job.name=eagletest, hive.optimize.skewjoin.compiletime=false, hive.query.string=insert overwrite table xxxx}", jobIdToJobConfig.get(JOB_ID).toString());
        Assert.assertTrue(curator.checkExists().forPath(ZK_JOB_PATH) != null);
        Assert.assertTrue(curator.checkExists().forPath(ZK_APP_PATH) != null);
        Assert.assertTrue(curator.checkExists().forPath(ZKROOT) != null);
        Assert.assertEquals(DATA_FROM_ZK, new String(curator.getData().forPath(ZK_JOB_PATH), "UTF-8"));
        Assert.assertTrue(entities.isEmpty());
        verify(client, times(1)).create(any());

    }

    @Test
    public void testMRJobParserFetchMrJobFail() throws Exception {
        setupMock();
        mockInputJobSteamWithException(JOB_URL);
        mockGetConnection("/mrconf_30784.xml");


        Assert.assertTrue(curator.checkExists().forPath(ZKROOT) == null);
        Assert.assertTrue(curator.checkExists().forPath(ZK_JOB_PATH) == null);
        List<String> confKeyKeys = makeConfKeyKeys(mrRunningJobConfig);

        InputStream previousmrrunningapp = this.getClass().getResourceAsStream("/previousmrrunningapp.json");
        AppsWrapper appsWrapper = OBJ_MAPPER.readValue(previousmrrunningapp, AppsWrapper.class);
        List<AppInfo> appInfos = appsWrapper.getApps().getApp();
        AppInfo app1 = appInfos.get(0);
        Map<String, JobExecutionAPIEntity> mrJobs = null;

        MRRunningJobManager runningJobManager = new MRRunningJobManager(mrRunningJobConfig.getZkStateConfig());
        RMResourceFetcher resourceFetcher = new RMResourceFetcher(mrRunningJobConfig.getEndpointConfig().rmUrls);
        MRJobParser mrJobParser = new MRJobParser(mrRunningJobConfig.getEndpointConfig(), mrRunningJobConfig.getEagleServiceConfig(),
                app1, mrJobs, runningJobManager, resourceFetcher, confKeyKeys, config);


        Map<String, JobExecutionAPIEntity> jobIdToJobExecutionAPIEntity = getMrJobs(mrJobParser);
        Map<String, JobConfig> jobIdToJobConfig = getMrJobConfigs(mrJobParser);
        MRJobEntityCreationHandler mrJobEntityCreationHandler = getMrJobEntityCreationHandler(mrJobParser);
        List<TaggedLogAPIEntity> entities = getMrJobEntityCreationHandlerEntities(mrJobEntityCreationHandler);
        Assert.assertTrue(jobIdToJobExecutionAPIEntity.isEmpty());
        Assert.assertTrue(jobIdToJobConfig.isEmpty());
        Assert.assertTrue(entities.isEmpty());
        Assert.assertTrue(mrJobParser.status() == MRJobParser.ParserStatus.FINISHED);

        mrJobParser.run();

        Assert.assertTrue(jobIdToJobExecutionAPIEntity.size() == 0);
        Assert.assertTrue(mrJobParser.status() == MRJobParser.ParserStatus.FINISHED);
        Assert.assertTrue(curator.checkExists().forPath(ZK_JOB_PATH) == null);
        Assert.assertTrue(curator.checkExists().forPath(ZK_APP_PATH) == null);
        Assert.assertTrue(entities.isEmpty());
        verify(client, never()).create(any());
    }

    @Test
    public void testMRJobParserFetchJobConfFail() throws Exception {
        setupMock();
        mockInputJobSteam("/mrjob_30784.json", JOB_URL);
        mockGetConnectionWithException("/mrconf_30784.xml");


        Assert.assertTrue(curator.checkExists().forPath(ZKROOT) == null);
        Assert.assertTrue(curator.checkExists().forPath(ZK_JOB_PATH) == null);
        List<String> confKeyKeys = makeConfKeyKeys(mrRunningJobConfig);

        InputStream previousmrrunningapp = this.getClass().getResourceAsStream("/previousmrrunningapp.json");
        AppsWrapper appsWrapper = OBJ_MAPPER.readValue(previousmrrunningapp, AppsWrapper.class);
        List<AppInfo> appInfos = appsWrapper.getApps().getApp();
        AppInfo app1 = appInfos.get(0);
        Map<String, JobExecutionAPIEntity> mrJobs = null;

        MRRunningJobManager runningJobManager = new MRRunningJobManager(mrRunningJobConfig.getZkStateConfig());
        RMResourceFetcher resourceFetcher = new RMResourceFetcher(mrRunningJobConfig.getEndpointConfig().rmUrls);
        MRJobParser mrJobParser = new MRJobParser(mrRunningJobConfig.getEndpointConfig(), mrRunningJobConfig.getEagleServiceConfig(),
                app1, mrJobs, runningJobManager, resourceFetcher, confKeyKeys, config);


        Map<String, JobExecutionAPIEntity> jobIdToJobExecutionAPIEntity = getMrJobs(mrJobParser);
        Map<String, JobConfig> jobIdToJobConfig = getMrJobConfigs(mrJobParser);
        MRJobEntityCreationHandler mrJobEntityCreationHandler = getMrJobEntityCreationHandler(mrJobParser);
        List<TaggedLogAPIEntity> entities = getMrJobEntityCreationHandlerEntities(mrJobEntityCreationHandler);
        Assert.assertTrue(jobIdToJobExecutionAPIEntity.isEmpty());
        Assert.assertTrue(jobIdToJobConfig.isEmpty());
        Assert.assertTrue(entities.isEmpty());
        Assert.assertTrue(mrJobParser.status() == MRJobParser.ParserStatus.FINISHED);

        mrJobParser.run();

        Assert.assertTrue(jobIdToJobExecutionAPIEntity.size() == 1);
        Assert.assertTrue(mrJobParser.status() == MRJobParser.ParserStatus.FINISHED);
        Assert.assertTrue(curator.checkExists().forPath(ZK_JOB_PATH) == null);
        Assert.assertTrue(curator.checkExists().forPath(ZK_APP_PATH) == null);
        Assert.assertTrue(entities.isEmpty());
        verify(client, times(1)).create(any());
    }


    @Test
    public void testMRJobParserFetchJobCountFail() throws Exception {
        setupMock();
        mockInputJobSteam("/mrjob_30784.json", JOB_URL);
        mockGetConnection("/mrconf_30784.xml");
        mockInputJobSteamWithException(JOB_COUNT_URL);


        Assert.assertTrue(curator.checkExists().forPath(ZKROOT) == null);
        Assert.assertTrue(curator.checkExists().forPath(ZK_JOB_PATH) == null);
        List<String> confKeyKeys = makeConfKeyKeys(mrRunningJobConfig);

        InputStream previousmrrunningapp = this.getClass().getResourceAsStream("/previousmrrunningapp.json");
        AppsWrapper appsWrapper = OBJ_MAPPER.readValue(previousmrrunningapp, AppsWrapper.class);
        List<AppInfo> appInfos = appsWrapper.getApps().getApp();
        AppInfo app1 = appInfos.get(0);
        Map<String, JobExecutionAPIEntity> mrJobs = null;

        MRRunningJobManager runningJobManager = new MRRunningJobManager(mrRunningJobConfig.getZkStateConfig());
        RMResourceFetcher resourceFetcher = new RMResourceFetcher(mrRunningJobConfig.getEndpointConfig().rmUrls);
        MRJobParser mrJobParser = new MRJobParser(mrRunningJobConfig.getEndpointConfig(), mrRunningJobConfig.getEagleServiceConfig(),
                app1, mrJobs, runningJobManager, resourceFetcher, confKeyKeys, config);


        Map<String, JobExecutionAPIEntity> jobIdToJobExecutionAPIEntity = getMrJobs(mrJobParser);
        Map<String, JobConfig> jobIdToJobConfig = getMrJobConfigs(mrJobParser);
        MRJobEntityCreationHandler mrJobEntityCreationHandler = getMrJobEntityCreationHandler(mrJobParser);
        List<TaggedLogAPIEntity> entities = getMrJobEntityCreationHandlerEntities(mrJobEntityCreationHandler);
        Assert.assertTrue(jobIdToJobExecutionAPIEntity.isEmpty());
        Assert.assertTrue(jobIdToJobConfig.isEmpty());
        Assert.assertTrue(entities.isEmpty());
        Assert.assertTrue(mrJobParser.status() == MRJobParser.ParserStatus.FINISHED);

        mrJobParser.run();

        Assert.assertTrue(jobIdToJobExecutionAPIEntity.size() == 1);
        Assert.assertTrue(mrJobParser.status() == MRJobParser.ParserStatus.FINISHED);
        Assert.assertTrue(curator.checkExists().forPath(ZK_JOB_PATH) != null);
        Assert.assertTrue(curator.checkExists().forPath(ZK_APP_PATH) != null);
        Assert.assertTrue(curator.checkExists().forPath(ZKROOT) != null);
        Assert.assertEquals(DATA_FROM_ZK, new String(curator.getData().forPath(ZK_JOB_PATH), "UTF-8"));
        Assert.assertTrue(entities.isEmpty());
        verify(client, times(1)).create(any());
    }

    @Test
    public void testMRJobParserFetchJobConfFailButRMalive() throws Exception {
        setupMock();
        mockInputJobSteam("/mrjob_30784.json", JOB_URL);
        mockGetConnectionWithException("/mrconf_30784.xml");
        mockInputJobSteamWithException(JOB_COUNT_URL);


        Assert.assertTrue(curator.checkExists().forPath(ZKROOT) == null);
        Assert.assertTrue(curator.checkExists().forPath(ZK_JOB_PATH) == null);
        List<String> confKeyKeys = makeConfKeyKeys(mrRunningJobConfig);

        InputStream previousmrrunningapp = this.getClass().getResourceAsStream("/previousmrrunningapp.json");
        AppsWrapper appsWrapper = OBJ_MAPPER.readValue(previousmrrunningapp, AppsWrapper.class);
        List<AppInfo> appInfos = appsWrapper.getApps().getApp();
        AppInfo app1 = appInfos.get(0);
        Map<String, JobExecutionAPIEntity> mrJobs = null;

        MRRunningJobManager runningJobManager = new MRRunningJobManager(mrRunningJobConfig.getZkStateConfig());
        RMResourceFetcher resourceFetcher = mock(RMResourceFetcher.class);
        when(resourceFetcher.getResource(any())).thenReturn(Collections.emptyList());
        MRJobParser mrJobParser = new MRJobParser(mrRunningJobConfig.getEndpointConfig(), mrRunningJobConfig.getEagleServiceConfig(),
                app1, mrJobs, runningJobManager, resourceFetcher, confKeyKeys, config);


        Map<String, JobExecutionAPIEntity> jobIdToJobExecutionAPIEntity = getMrJobs(mrJobParser);
        Map<String, JobConfig> jobIdToJobConfig = getMrJobConfigs(mrJobParser);
        MRJobEntityCreationHandler mrJobEntityCreationHandler = getMrJobEntityCreationHandler(mrJobParser);
        List<TaggedLogAPIEntity> entities = getMrJobEntityCreationHandlerEntities(mrJobEntityCreationHandler);
        Assert.assertTrue(jobIdToJobExecutionAPIEntity.isEmpty());
        Assert.assertTrue(jobIdToJobConfig.isEmpty());
        Assert.assertTrue(entities.isEmpty());
        Assert.assertTrue(mrJobParser.status() == MRJobParser.ParserStatus.FINISHED);

        mrJobParser.run();

        Assert.assertTrue(jobIdToJobConfig.isEmpty());
        Assert.assertTrue(jobIdToJobExecutionAPIEntity.size() == 1);
        JobExecutionAPIEntity jobExecutionAPIEntity = jobIdToJobExecutionAPIEntity.get(JOB_ID);
        Assert.assertEquals(Constants.AppState.FINISHED.toString(), jobExecutionAPIEntity.getInternalState());
        Assert.assertEquals(Constants.AppState.RUNNING.toString(), jobExecutionAPIEntity.getCurrentState());
        Assert.assertTrue(mrJobParser.status() == MRJobParser.ParserStatus.APP_FINISHED);
        Assert.assertTrue(curator.checkExists().forPath(ZK_JOB_PATH) == null);
        Assert.assertTrue(curator.checkExists().forPath(ZK_APP_PATH) == null);
        Assert.assertTrue(entities.isEmpty());
        verify(client, times(1)).create(any());
    }


    @Test
    public void testMRJobParserFetchJobCountFailButRMaliveRetry() throws Exception {
        setupMock();
        reset(client);
        client = mock(EagleServiceClientImpl.class);
        MRRunningJobConfig.EagleServiceConfig eagleServiceConfig = mrRunningJobConfig.getEagleServiceConfig();
        PowerMockito.whenNew(EagleServiceClientImpl.class).withArguments(
            eagleServiceConfig.eagleServiceHost,
            eagleServiceConfig.eagleServicePort,
            eagleServiceConfig.username,
            eagleServiceConfig.password).thenReturn(client);
        when(client.create(any())).thenThrow(Exception.class).thenReturn(null);
        when(client.getJerseyClient()).thenReturn(new Client());
        mockInputJobSteam("/mrjob_30784.json", JOB_URL);
        mockInputJobSteamWithException(JOB_COUNT_URL);
        mockGetConnection("/mrconf_30784.xml");


        Assert.assertTrue(curator.checkExists().forPath(ZKROOT) == null);
        Assert.assertTrue(curator.checkExists().forPath(ZK_JOB_PATH) == null);
        List<String> confKeyKeys = makeConfKeyKeys(mrRunningJobConfig);

        InputStream previousmrrunningapp = this.getClass().getResourceAsStream("/previousmrrunningapp.json");
        AppsWrapper appsWrapper = OBJ_MAPPER.readValue(previousmrrunningapp, AppsWrapper.class);
        List<AppInfo> appInfos = appsWrapper.getApps().getApp();
        AppInfo app1 = appInfos.get(0);
        Map<String, JobExecutionAPIEntity> mrJobs = null;

        MRRunningJobManager runningJobManager = new MRRunningJobManager(mrRunningJobConfig.getZkStateConfig());
        RMResourceFetcher resourceFetcher = mock(RMResourceFetcher.class);
        when(resourceFetcher.getResource(any())).thenReturn(Collections.emptyList());
        MRJobParser mrJobParser = new MRJobParser(mrRunningJobConfig.getEndpointConfig(), mrRunningJobConfig.getEagleServiceConfig(),
                app1, mrJobs, runningJobManager, resourceFetcher, confKeyKeys, config);


        Map<String, JobExecutionAPIEntity> jobIdToJobExecutionAPIEntity = getMrJobs(mrJobParser);
        Map<String, JobConfig> jobIdToJobConfig = getMrJobConfigs(mrJobParser);
        MRJobEntityCreationHandler mrJobEntityCreationHandler = getMrJobEntityCreationHandler(mrJobParser);
        List<TaggedLogAPIEntity> entities = getMrJobEntityCreationHandlerEntities(mrJobEntityCreationHandler);
        Assert.assertTrue(jobIdToJobExecutionAPIEntity.isEmpty());
        Assert.assertTrue(jobIdToJobConfig.isEmpty());
        Assert.assertTrue(entities.isEmpty());
        Assert.assertTrue(mrJobParser.status() == MRJobParser.ParserStatus.FINISHED);

        mrJobParser.run();

        Assert.assertTrue(jobIdToJobConfig.isEmpty());
        Assert.assertTrue(jobIdToJobExecutionAPIEntity.size() == 1);
        JobExecutionAPIEntity jobExecutionAPIEntity = jobIdToJobExecutionAPIEntity.get(JOB_ID);
        Assert.assertEquals(Constants.AppState.FINISHED.toString(), jobExecutionAPIEntity.getInternalState());
        Assert.assertEquals(Constants.AppState.RUNNING.toString(), jobExecutionAPIEntity.getCurrentState());
        Assert.assertTrue(mrJobParser.status() == MRJobParser.ParserStatus.APP_FINISHED);
        Assert.assertTrue(curator.checkExists().forPath(ZK_JOB_PATH) == null);
        Assert.assertTrue(curator.checkExists().forPath(ZK_APP_PATH) == null);
        Assert.assertTrue(entities.isEmpty());
        verify(client, times(2)).create(any());
        verify(client, times(1)).getJerseyClient();
        verify(client, times(1)).close();

    }

    private void setupMock() throws Exception {
        mockStatic(Math.class);
        when(Math.random()).thenReturn(0.3689680489913364d);
        mockStatic(InputStreamUtils.class);
        client = mock(EagleServiceClientImpl.class);
        MRRunningJobConfig.EagleServiceConfig eagleServiceConfig = mrRunningJobConfig.getEagleServiceConfig();
        PowerMockito.whenNew(EagleServiceClientImpl.class).withArguments(
                eagleServiceConfig.eagleServiceHost,
                eagleServiceConfig.eagleServicePort,
                eagleServiceConfig.username,
                eagleServiceConfig.password).thenReturn(client);
        when(client.create(any())).thenReturn(null);
        when(client.getJerseyClient()).thenReturn(new Client());

    }

    private Map<String, JobConfig> getMrJobConfigs(MRJobParser mrJobParser) throws NoSuchFieldException, IllegalAccessException {
        Field mrJobConfigs = MRJobParser.class.getDeclaredField("mrJobConfigs");
        mrJobConfigs.setAccessible(true);
        return (Map<String, JobConfig>) mrJobConfigs.get(mrJobParser);
    }

    private Map<String, JobExecutionAPIEntity> getMrJobs(MRJobParser mrJobParser) throws NoSuchFieldException, IllegalAccessException {
        Field mrJobEntityMap = MRJobParser.class.getDeclaredField("mrJobEntityMap");
        mrJobEntityMap.setAccessible(true);
        return (Map<String, JobExecutionAPIEntity>) mrJobEntityMap.get(mrJobParser);
    }

    private MRJobEntityCreationHandler getMrJobEntityCreationHandler(MRJobParser mrJobParser) throws NoSuchFieldException, IllegalAccessException {
        Field mrJobEntityCreationHandler = MRJobParser.class.getDeclaredField("mrJobEntityCreationHandler");
        mrJobEntityCreationHandler.setAccessible(true);
        return (MRJobEntityCreationHandler) mrJobEntityCreationHandler.get(mrJobParser);
    }

    private List<TaggedLogAPIEntity> getMrJobEntityCreationHandlerEntities(MRJobEntityCreationHandler mrJobEntityCreationHandler) throws NoSuchFieldException, IllegalAccessException {
        Field entities = MRJobEntityCreationHandler.class.getDeclaredField("entities");
        entities.setAccessible(true);
        return (ArrayList<TaggedLogAPIEntity>) entities.get(mrJobEntityCreationHandler);
    }

    private void initMrJobEntityCreationHandlerEntities(MRJobEntityCreationHandler mrJobEntityCreationHandler) throws NoSuchFieldException, IllegalAccessException {
        Field entities = MRJobEntityCreationHandler.class.getDeclaredField("entities");
        entities.setAccessible(true);
        entities.set(mrJobEntityCreationHandler, new ArrayList<>());
    }

    private void initMetricsCreationListener(MRJobEntityCreationHandler mrJobEntityCreationHandler) throws IllegalAccessException, NoSuchFieldException {

        Field jobMetricsListener = MRJobEntityCreationHandler.class.getDeclaredField("jobMetricsListener");
        jobMetricsListener.setAccessible(true);
        jobMetricsListener.set(mrJobEntityCreationHandler, new JobExecutionMetricsCreationListener());
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
        confKeyKeys.add("hive.optimize.skewjoin.compiletime");
        confKeyKeys.add(0, mrRunningJobConfig.getConfig().getString("MRConfigureKeys.jobNameKey"));
        return confKeyKeys;
    }

    private void mockInputJobSteam(String mockDataFilePath, String url) throws Exception {
        InputStream jsonstream = this.getClass().getResourceAsStream(mockDataFilePath);
        when(InputStreamUtils.getInputStream(eq(url), anyObject(), anyObject())).thenReturn(jsonstream);
    }

    private void mockInputJobSteamWithException(String url) throws Exception {
        when(InputStreamUtils.getInputStream(eq(url), anyObject(), anyObject())).thenThrow(new Exception());
    }

    private void mockGetConnectionWithException(String mockDataFilePath) throws Exception {
        InputStream jsonstream = this.getClass().getResourceAsStream(mockDataFilePath);
        mockStatic(URLConnectionUtils.class);
        URLConnection connection = mock(URLConnection.class);
        when(connection.getInputStream()).thenReturn(jsonstream);
        when(URLConnectionUtils.getConnection(JOB_CONF_URL)).thenThrow(new Exception());
    }

    private void mockGetConnection(String mockDataFilePath) throws Exception {
        InputStream jsonstream = this.getClass().getResourceAsStream(mockDataFilePath);
        mockStatic(URLConnectionUtils.class);
        URLConnection connection = mock(URLConnection.class);
        when(connection.getInputStream()).thenReturn(jsonstream);
        when(URLConnectionUtils.getConnection(JOB_CONF_URL)).thenReturn(connection);
    }
}
