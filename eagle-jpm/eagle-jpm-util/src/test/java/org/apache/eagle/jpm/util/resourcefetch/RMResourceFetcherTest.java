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

package org.apache.eagle.jpm.util.resourcefetch;

import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourcefetch.connection.InputStreamUtils;
import org.apache.eagle.jpm.util.resourcefetch.ha.HAURLSelectorImpl;
import org.apache.eagle.jpm.util.resourcefetch.model.AppInfo;
import org.apache.eagle.jpm.util.resourcefetch.model.ClusterInfo;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.InputStream;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest(InputStreamUtils.class)
public class RMResourceFetcherTest {
    private InputStream clusterInfoStream = this.getClass().getResourceAsStream("/clusterinfo.json");

    @Test
    public void testCompleteMrJob() throws Exception {
        String[] rmBasePaths = new String[]{"http://www.xxx.com:8088", "http://www.yyy.com:8088"};
        RMResourceFetcher rmResourceFetcher = new RMResourceFetcher(rmBasePaths);
        InputStream jsonstream = this.getClass().getResourceAsStream("/mrcompleteapp.json");
        mockStatic(InputStreamUtils.class);
        when(InputStreamUtils.getInputStream("http://www.xxx.com:8088/ws/v1/cluster?anonymous=true", null, Constants.CompressionType.NONE)).thenReturn(clusterInfoStream);
        when(InputStreamUtils.getInputStream("http://www.xxx.com:8088/ws/v1/cluster/apps?applicationTypes=MAPREDUCE&state=FINISHED&finishedTimeBegin=1479244718794&anonymous=true", null, Constants.CompressionType.GZIP)).thenReturn(jsonstream);
        String lastFinishedTime = "1479244718794";
        List<AppInfo> appInfos = rmResourceFetcher.getResource(Constants.ResourceType.COMPLETE_MR_JOB, lastFinishedTime);
        Assert.assertEquals(2, appInfos.size());
        Assert.assertEquals("AppInfo{id='application_1326815542473_0001', user='user1', name='insert overwrite table xx.yy...end(Stage-1)', queue='default', state='FINISHED', finalStatus='SUCCEEDED', progress=100.0, trackingUI='History', trackingUrl='http://host.domain.com:8088/proxy/application_1326815542473_0001/jobhistory/job/job_1326815542473_1_1', diagnostics='diagnostics1', clusterId='1326815542473', applicationType='MAPREDUCE', startedTime=1326815573334, finishedTime=1479244718794, elapsedTime=25196, amContainerLogs='http://host.domain.com:8042/node/containerlogs/container_1326815542473_0001_01_000001', amHostHttpAddress='host.domain.com:8042', allocatedMB=0, allocatedVCores=0, runningContainers=0}", appInfos.get(0).toString());
        Assert.assertEquals("AppInfo{id='application_1326815542473_0002', user='user1', name='Sleep job', queue='default', state='FINISHED', finalStatus='SUCCEEDED', progress=100.0, trackingUI='History', trackingUrl='http://host.domain.com:8088/proxy/application_1326815542473_0002/jobhistory/job/job_1326815542473_2_2', diagnostics='diagnostics2', clusterId='1326815542473', applicationType='MAPREDUCE', startedTime=1326815641380, finishedTime=1326815789546, elapsedTime=148166, amContainerLogs='http://host.domain.com:8042/node/containerlogs/container_1326815542473_0002_01_000001', amHostHttpAddress='host.domain.com:8042', allocatedMB=0, allocatedVCores=0, runningContainers=1}", appInfos.get(1).toString());
    }

    @Test
    public void testRunningMrJob() throws Exception {
        String[] rmBasePaths = new String[]{"http://www.xxx.com:8088", "http://www.yyy.com:8088"};
        RMResourceFetcher rmResourceFetcher = new RMResourceFetcher(rmBasePaths);
        InputStream jsonstream = this.getClass().getResourceAsStream("/mrrunningapp.json");
        mockStatic(InputStreamUtils.class);
        when(InputStreamUtils.getInputStream("http://www.xxx.com:8088/ws/v1/cluster?anonymous=true", null, Constants.CompressionType.NONE)).thenReturn(clusterInfoStream);
        when(InputStreamUtils.getInputStream("http://www.xxx.com:8088/ws/v1/cluster/apps?applicationTypes=MAPREDUCE&state=RUNNING&anonymous=true", null, Constants.CompressionType.GZIP)).thenReturn(jsonstream);
        List<AppInfo> appInfos = rmResourceFetcher.getResource(Constants.ResourceType.RUNNING_MR_JOB);
        Assert.assertEquals(2, appInfos.size());
        Assert.assertEquals("AppInfo{id='application_1479206441898_30784', user='xxx', name='oozie:launcher:T=shell:W=wf_co_xxx_xxx_v3:A=extract_org_data:ID=0002383-161115184801730-oozie-oozi-W', queue='xxx', state='RUNNING', finalStatus='UNDEFINED', progress=95.0, trackingUI='ApplicationMaster', trackingUrl='http://host.domain.com:8088/proxy/application_1479206441898_30784/', diagnostics='', clusterId='1479206441898', applicationType='MAPREDUCE', startedTime=1479328221694, finishedTime=0, elapsedTime=13367402, amContainerLogs='http://host.domain.com:8088/node/containerlogs/container_e11_1479206441898_30784_01_000001/xxx', amHostHttpAddress='host.domain.com:8088', allocatedMB=3072, allocatedVCores=2, runningContainers=2}", appInfos.get(0).toString());
        Assert.assertEquals("AppInfo{id='application_1479206441898_35341', user='yyy', name='insert overwrite table inter...a.xxx(Stage-3)', queue='yyy', state='RUNNING', finalStatus='UNDEFINED', progress=59.545456, trackingUI='ApplicationMaster', trackingUrl='http://host.domain.com:8088/proxy/application_1479206441898_35341/', diagnostics='', clusterId='1479206441898', applicationType='MAPREDUCE', startedTime=1479341511477, finishedTime=0, elapsedTime=77619, amContainerLogs='http://host.domain.com:8042/node/containerlogs/container_e11_1479206441898_35341_01_000005/yyy', amHostHttpAddress='host.domain.com:8042', allocatedMB=27648, allocatedVCores=6, runningContainers=6}", appInfos.get(1).toString());
    }

    @Test
    public void testSparkRunningJob() throws Exception {
        String[] rmBasePaths = new String[]{"http://www.xxx.com:8088", "http://www.yyy.com:8088"};
        RMResourceFetcher rmResourceFetcher = new RMResourceFetcher(rmBasePaths);
        InputStream jsonstream = this.getClass().getResourceAsStream("/sparkrunningapp.json");
        mockStatic(InputStreamUtils.class);
        when(InputStreamUtils.getInputStream("http://www.xxx.com:8088/ws/v1/cluster/apps?applicationTypes=SPARK&state=RUNNING&anonymous=true", null, Constants.CompressionType.GZIP)).thenReturn(jsonstream);
        when(InputStreamUtils.getInputStream("http://www.xxx.com:8088/ws/v1/cluster?anonymous=true", null, Constants.CompressionType.NONE)).thenReturn(clusterInfoStream);

        List<AppInfo> appInfos = rmResourceFetcher.getResource(Constants.ResourceType.RUNNING_SPARK_JOB);
        Assert.assertEquals(2, appInfos.size());
        Assert.assertEquals("AppInfo{id='application_1479196259121_1041', user='xxx', name='Spark shell', queue='default', state='RUNNING', finalStatus='UNDEFINED', progress=10.0, trackingUI='ApplicationMaster', trackingUrl='http://host.domain.com:8088/proxy/application_1479196259121_1041/', diagnostics='', clusterId='1479206441898', applicationType='SPARK', startedTime=1479202371723, finishedTime=0, elapsedTime=147954814, amContainerLogs='http://host.domain.com:8042/node/containerlogs/container_e09_1479196259121_1041_01_000001/xxx', amHostHttpAddress='host.domain.com:8042', allocatedMB=7168, allocatedVCores=3, runningContainers=3}", appInfos.get(0).toString());
        Assert.assertEquals("AppInfo{id='application_1478655515028_74912', user='xxx', name='spark-thriftserver', queue='default', state='RUNNING', finalStatus='UNDEFINED', progress=10.0, trackingUI='ApplicationMaster', trackingUrl='http://host.domain.com:8088/proxy/application_1478655515028_74912/', diagnostics='', clusterId='1479206441898', applicationType='SPARK', startedTime=1478844771535, finishedTime=0, elapsedTime=505555002, amContainerLogs='http://host.domain.com:8042/node/containerlogs/container_e10_1478655515028_74912_02_000001/xxx', amHostHttpAddress='host.domain.com:8042', allocatedMB=3113984, allocatedVCores=191, runningContainers=191}", appInfos.get(1).toString());
    }

    @Test
    public void testCompleteSparkJob() throws Exception {
        String[] rmBasePaths = new String[]{"http://www.xxx.com:8088", "http://www.yyy.com:8088"};
        RMResourceFetcher rmResourceFetcher = new RMResourceFetcher(rmBasePaths);
        InputStream jsonstream = this.getClass().getResourceAsStream("/sparkcompleteapp.json");
        mockStatic(InputStreamUtils.class);
        long finishedTimeBegin = 1479244718794l;
        when(InputStreamUtils.getInputStream("http://www.xxx.com:8088/ws/v1/cluster?anonymous=true", null, Constants.CompressionType.NONE)).thenReturn(clusterInfoStream);
        when(InputStreamUtils.getInputStream("http://www.xxx.com:8088/ws/v1/cluster/apps?applicationTypes=SPARK&state=FINISHED&finishedTimeBegin=1479244718794&anonymous=true", null, Constants.CompressionType.GZIP)).thenReturn(jsonstream);
        List<AppInfo> appInfos = rmResourceFetcher.getResource(Constants.ResourceType.COMPLETE_SPARK_JOB, String.valueOf(finishedTimeBegin));
        Assert.assertEquals(2, appInfos.size());
        Assert.assertEquals("AppInfo{id='application_1479196259121_1041', user='xxx', name='Spark shell', queue='default', state='FINISHED', finalStatus='SUCCEEDED', progress=10.0, trackingUI='ApplicationMaster', trackingUrl='http://host.domain.com:8088/proxy/application_1479196259121_1041/', diagnostics='', clusterId='1479206441898', applicationType='SPARK', startedTime=1479202371723, finishedTime=1479244718794, elapsedTime=147954814, amContainerLogs='http://host.domain.com:8042/node/containerlogs/container_e09_1479196259121_1041_01_000001/xxx', amHostHttpAddress='host.domain.com:8042', allocatedMB=7168, allocatedVCores=3, runningContainers=3}", appInfos.get(0).toString());
        Assert.assertEquals("AppInfo{id='application_1478655515028_74912', user='xxx', name='spark-thriftserver', queue='default', state='FINISHED', finalStatus='SUCCEEDED', progress=10.0, trackingUI='ApplicationMaster', trackingUrl='http://host.domain.com:8088/proxy/application_1478655515028_74912/', diagnostics='', clusterId='1479206441898', applicationType='SPARK', startedTime=1478844771535, finishedTime=1479244718794, elapsedTime=505555002, amContainerLogs='http://host.domain.com:8042/node/containerlogs/container_e10_1478655515028_74912_02_000001/xxx', amHostHttpAddress='host.domain.com:8042', allocatedMB=3113984, allocatedVCores=191, runningContainers=191}", appInfos.get(1).toString());
    }


    @Test
    public void testGetClusterInfo() throws Exception {
        String[] rmBasePaths = new String[]{"http://www.xxx.com:8088", "http://www.yyy.com:8088"};
        RMResourceFetcher rmResourceFetcher = new RMResourceFetcher(rmBasePaths);
        mockStatic(InputStreamUtils.class);
        InputStream jsonstream = this.getClass().getResourceAsStream("/clusterinfo.json");
        when(InputStreamUtils.getInputStream("http://www.xxx.com:8088/ws/v1/cluster?anonymous=true", null, Constants.CompressionType.NONE)).thenReturn(clusterInfoStream);
        when(InputStreamUtils.getInputStream("http://www.xxx.com:8088/ws/v1/cluster/info?anonymous=true", null, Constants.CompressionType.GZIP)).thenReturn(jsonstream);
        ClusterInfo clusterInfo = rmResourceFetcher.getClusterInfo();
        Assert.assertEquals("ClusterInfo{id=1324053971963, startedOn=1324053971963, state='STARTED', haState='ACTIVE', resourceManagerVersion='0.23.1-SNAPSHOT', resourceManagerBuildVersion='0.23.1-SNAPSHOT from 1214049 by user1 source checksum 050cd664439d931c8743a6428fd6a693', resourceManagerVersionBuiltOn='Tue Dec 13 22:12:48 CST 2011', hadoopVersion='0.23.1-SNAPSHOT', hadoopBuildVersion='0.23.1-SNAPSHOT from 1214049 by user1 source checksum 11458df3bb77342dca5f917198fad328', hadoopVersionBuiltOn='Tue Dec 13 22:12:26 CST 2011'}", clusterInfo.toString());
    }
}
