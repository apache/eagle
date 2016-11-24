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
package org.apache.eagle.jpm.mr.running;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.jpm.mr.running.recover.MRRunningJobManager;
import org.apache.eagle.jpm.mr.running.storm.MRRunningJobFetchSpout;
import org.apache.eagle.jpm.mr.runningentity.JobExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourcefetch.connection.InputStreamUtils;
import org.apache.eagle.jpm.util.resourcefetch.model.AppInfo;
import org.apache.eagle.jpm.util.resourcefetch.model.AppsWrapper;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({InputStreamUtils.class, MRRunningJobFetchSpout.class})
@PowerMockIgnore({"javax.*"})
public class MRRunningJobApplicationTest {

    public static final String RM_URL = "http://sandbox.hortonworks.com:50030/ws/v1/cluster/apps?applicationTypes=MAPREDUCE&state=RUNNING&anonymous=true";
    public static final String RUNNING_YARNAPPS = "[application_1479206441898_35341, application_1479206441898_30784]";
    public static final String TUPLE_1 = "[application_1479206441898_30784, AppInfo{id='application_1479206441898_30784', user='xxx', name='oozie:launcher:T=shell:W=wf_co_xxx_xxx_v3:A=extract_org_data:ID=0002383-161115184801730-oozie-oozi-W', queue='xxx', state='RUNNING', finalStatus='UNDEFINED', progress=95.0, trackingUI='ApplicationMaster', trackingUrl='http://host.domain.com:8088/proxy/application_1479206441898_30784/', diagnostics='', clusterId='1479206441898', applicationType='MAPREDUCE', startedTime=1479328221694, finishedTime=0, elapsedTime=13367402, amContainerLogs='http://host.domain.com:8088/node/containerlogs/container_e11_1479206441898_30784_01_000001/xxx', amHostHttpAddress='host.domain.com:8088', allocatedMB=3072, allocatedVCores=2, runningContainers=2}, null]";
    public static final String TUPLE_2 = "[application_1479206441898_35341, AppInfo{id='application_1479206441898_35341', user='yyy', name='insert overwrite table inter...a.xxx(Stage-3)', queue='yyy', state='RUNNING', finalStatus='UNDEFINED', progress=59.545456, trackingUI='ApplicationMaster', trackingUrl='http://host.domain.com:8088/proxy/application_1479206441898_35341/', diagnostics='', clusterId='1479206441898', applicationType='MAPREDUCE', startedTime=1479341511477, finishedTime=0, elapsedTime=77619, amContainerLogs='http://host.domain.com:8042/node/containerlogs/container_e11_1479206441898_35341_01_000005/yyy', amHostHttpAddress='host.domain.com:8042', allocatedMB=27648, allocatedVCores=6, runningContainers=6}, null]";

    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    static {
        OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    @Test
    public void testMRRunningJobFetchSpout() throws Exception {

        List<Object> tuples = new ArrayList<>();
        SpoutOutputCollector collector = new SpoutOutputCollector(new ISpoutOutputCollector() {
            @Override
            public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
                tuples.add(tuple);
                return null;
            }

            @Override
            public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {

            }

            @Override
            public void reportError(Throwable error) {

            }
        });

        //1st run
        Field initField = MRRunningJobFetchSpout.class.getDeclaredField("init");
        initField.setAccessible(true);
        MRRunningJobFetchSpout mrRunningJobFetchSpout = makeMrRunningJobFetchSpout();
        boolean init = (boolean) initField.get(mrRunningJobFetchSpout);
        mrRunningJobFetchSpout.open(new HashMap<>(), null, collector);
        Assert.assertFalse(init);
        mrRunningJobFetchSpout.nextTuple();

        init = (boolean) initField.get(mrRunningJobFetchSpout);
        Field runningYarnAppsField = MRRunningJobFetchSpout.class.getDeclaredField("runningYarnApps");
        runningYarnAppsField.setAccessible(true);
        Set<String> runningYarnApps = (Set<String>) runningYarnAppsField.get(mrRunningJobFetchSpout);
        Assert.assertTrue(tuples.isEmpty());
        Assert.assertTrue(init);
        Assert.assertTrue(runningYarnApps.isEmpty());

        //2nd run
        mrRunningJobFetchSpout.nextTuple();

        init = (boolean) initField.get(mrRunningJobFetchSpout);
        Assert.assertTrue(init);
        Assert.assertEquals(2, tuples.size());
        Assert.assertEquals(TUPLE_1, tuples.get(0).toString());
        Assert.assertEquals(TUPLE_2, tuples.get(1).toString());
        runningYarnApps = (Set<String>) runningYarnAppsField.get(mrRunningJobFetchSpout);
        Assert.assertEquals(2, runningYarnApps.size());
        Assert.assertEquals(RUNNING_YARNAPPS, runningYarnApps.toString());

        //3rd run
        mockInputSteam("/previousmrrunningapp.json");
        tuples.clear();

        mrRunningJobFetchSpout.nextTuple();

        Assert.assertTrue(init);
        Assert.assertEquals(2, tuples.size());
        Assert.assertEquals(TUPLE_1, tuples.get(0).toString());
        Assert.assertEquals(TUPLE_2, tuples.get(1).toString());
        runningYarnApps = (Set<String>) runningYarnAppsField.get(mrRunningJobFetchSpout);
        Assert.assertEquals(2, runningYarnApps.size());
        Assert.assertEquals(RUNNING_YARNAPPS, runningYarnApps.toString());

        //4th run
        mockInputSteam("/thistimemrrunningapp.json");
        tuples.clear();

        mrRunningJobFetchSpout.nextTuple();

        Assert.assertTrue(init);
        Assert.assertEquals(2, tuples.size());
        Assert.assertEquals(TUPLE_1, tuples.get(0).toString());
        Assert.assertEquals("[application_1479206441898_35341, AppInfo{id='application_1479206441898_35341', user='yyy', name='insert overwrite table inter...a.xxx(Stage-3)', queue='yyy', state='FINISHED', finalStatus='UNDEFINED', progress=59.545456, trackingUI='ApplicationMaster', trackingUrl='http://host.domain.com:8088/proxy/application_1479206441898_35341/', diagnostics='', clusterId='1479206441898', applicationType='MAPREDUCE', startedTime=1479341511477, finishedTime=0, elapsedTime=77619, amContainerLogs='http://host.domain.com:8042/node/containerlogs/container_e11_1479206441898_35341_01_000005/yyy', amHostHttpAddress='host.domain.com:8042', allocatedMB=27648, allocatedVCores=6, runningContainers=6}, {jobId=prefix:null, timestamp:0, humanReadableDate:1970-01-01 00:00:00,000, tags: , encodedRowkey:null}]", tuples.get(1).toString());

        runningYarnApps = (Set<String>) runningYarnAppsField.get(mrRunningJobFetchSpout);
        Assert.assertEquals(1, runningYarnApps.size());
        Assert.assertEquals("[application_1479206441898_30784]", runningYarnApps.toString());

    }

    private MRRunningJobFetchSpout makeMrRunningJobFetchSpout() throws Exception {

        mockInputSteam("/previousmrrunningapp.json");

        MRRunningJobConfig mrRunningJobConfig = MRRunningJobConfig.newInstance(ConfigFactory.load());
        mrRunningJobConfig.getEndpointConfig().fetchRunningJobInterval = 1;
        MRRunningJobManager mrRunningJobManager = mock(MRRunningJobManager.class);
        PowerMockito.whenNew(MRRunningJobManager.class).withArguments(mrRunningJobConfig.getZkStateConfig()).thenReturn(mrRunningJobManager);

        InputStream app35341 = this.getClass().getResourceAsStream("/application_1479206441898_35341.json");
        AppsWrapper appWrapper = OBJ_MAPPER.readValue(app35341, AppsWrapper.class);
        List<AppInfo> appInfos = appWrapper.getApps().getApp();
        Map<String, JobExecutionAPIEntity> jobs = new HashMap<>();
        JobExecutionAPIEntity jobExecutionAPIEntity = new JobExecutionAPIEntity();
        jobExecutionAPIEntity.setAppInfo(appInfos.get(0));
        jobs.put("jobId", jobExecutionAPIEntity);
        when(mrRunningJobManager.recoverYarnApp("application_1479206441898_35341")).thenReturn(jobs);

        return new MRRunningJobFetchSpout(mrRunningJobConfig.getEndpointConfig(), mrRunningJobConfig.getZkStateConfig());
    }
    private void mockInputSteam(String mockDataFilePath) throws Exception {
        InputStream jsonstream = this.getClass().getResourceAsStream(mockDataFilePath);
        mockStatic(InputStreamUtils.class);
        when(InputStreamUtils.getInputStream(RM_URL, null, Constants.CompressionType.GZIP)).thenReturn(jsonstream);
    }
}
