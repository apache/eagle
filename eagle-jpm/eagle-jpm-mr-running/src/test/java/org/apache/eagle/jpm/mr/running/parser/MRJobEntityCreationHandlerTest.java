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

import org.apache.eagle.jpm.mr.running.parser.metrics.JobExecutionMetricsCreationListener;
import org.apache.eagle.jpm.mr.runningentity.JobExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.Utils;
import org.apache.eagle.jpm.util.resourcefetch.connection.InputStreamUtils;
import org.apache.eagle.jpm.util.resourcefetch.connection.URLConnectionUtils;
import org.apache.eagle.jpm.util.resourcefetch.model.*;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({System.class, JobExecutionMetricsCreationListener.class, MRJobEntityCreationHandler.class})
public class MRJobEntityCreationHandlerTest {

    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    @BeforeClass
    public static void startZookeeper() throws Exception {
        OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    @Test
    public void testMRJobEntityCreationHandlerAdd() throws IOException, NoSuchFieldException, IllegalAccessException {
        mockStatic(System.class);
        when(System.currentTimeMillis()).thenReturn(1479863033310l);
        MRJobEntityCreationHandler mrJobEntityCreationHandler = new MRJobEntityCreationHandler(null);
        mrJobEntityCreationHandler.add(makeJobExecutionAPIEntity());

        Field entities = MRJobEntityCreationHandler.class.getDeclaredField("entities");
        entities.setAccessible(true);
        List<TaggedLogAPIEntity> entityList = (ArrayList<TaggedLogAPIEntity>) entities.get(mrJobEntityCreationHandler);
        Assert.assertEquals(4, entityList.size());
        Assert.assertEquals("[prefix:null, timestamp:1479328221694, humanReadableDate:2016-11-16 20:30:21,694, tags: , encodedRowkey:null, prefix:hadoop.job.allocatedmb, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.allocatedvcores, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.runningcontainers, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null]", entityList.toString());
    }

    @Test
    public void testMRJobEntityCreationHandlerWithCounts() throws IOException, NoSuchFieldException, IllegalAccessException {
        mockStatic(System.class);
        when(System.currentTimeMillis()).thenReturn(1479863033310l);
        MRJobEntityCreationHandler mrJobEntityCreationHandler = new MRJobEntityCreationHandler(null);
        mrJobEntityCreationHandler.add(makeJobExecutionAPIEntityWithCounts());

        Field entities = MRJobEntityCreationHandler.class.getDeclaredField("entities");
        entities.setAccessible(true);
        List<TaggedLogAPIEntity> entityList = (ArrayList<TaggedLogAPIEntity>) entities.get(mrJobEntityCreationHandler);
        Assert.assertEquals(62, entityList.size());
        Assert.assertEquals("[prefix:null, timestamp:1479328221694, humanReadableDate:2016-11-16 20:30:21,694, tags: , encodedRowkey:null, prefix:hadoop.job.allocatedmb, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.allocatedvcores, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.runningcontainers, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.viewfs_large_read_ops, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.file_bytes_written, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.file_large_read_ops, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.file_write_ops, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.viewfs_bytes_read, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.viewfs_read_ops, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.hdfs_read_ops, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.viewfs_write_ops, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.hdfs_bytes_read, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.hdfs_large_read_ops, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.file_read_ops, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.file_bytes_read, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.hdfs_write_ops, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.viewfs_bytes_written, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.hdfs_bytes_written, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.deserialize_errors, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.records_out_intermediate, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.records_in, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.bytes_written, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.bytes_read, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.total_launched_maps, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.vcores_millis_reduces, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.mb_millis_maps, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.total_launched_reduces, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.slots_millis_reduces, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.vcores_millis_maps, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.mb_millis_reduces, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.slots_millis_maps, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.rack_local_maps, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.millis_reduces, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.other_local_maps, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.millis_maps, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.map_output_materialized_bytes, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.reduce_input_records, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.spilled_records, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.merged_map_outputs, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.virtual_memory_bytes, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.map_input_records, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.split_raw_bytes, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.failed_shuffle, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.map_output_bytes, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.reduce_shuffle_bytes, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.physical_memory_bytes, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.gc_time_millis, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.reduce_input_groups, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.combine_output_records, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.shuffled_maps, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.reduce_output_records, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.map_output_records, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.combine_input_records, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.cpu_milliseconds, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.committed_heap_bytes, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.connection, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.wrong_length, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.bad_id, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.wrong_map, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.wrong_reduce, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null, prefix:hadoop.job.io_error, timestamp:1479863033310, humanReadableDate:2016-11-23 01:03:53,310, tags: , encodedRowkey:null]", entityList.toString());
    }

    private JobExecutionAPIEntity makeJobExecutionAPIEntity() throws IOException {
        InputStream jsonstream = this.getClass().getResourceAsStream("/mrjob_30784.json");
        List<MRJob> mrJobs = OBJ_MAPPER.readValue(jsonstream, MRJobsWrapper.class).getJobs().getJob();

        InputStream previousmrrunningapp = this.getClass().getResourceAsStream("/previousmrrunningapp.json");
        AppsWrapper appsWrapper = OBJ_MAPPER.readValue(previousmrrunningapp, AppsWrapper.class);
        List<AppInfo> appInfos = appsWrapper.getApps().getApp();
        AppInfo app = appInfos.get(0);
        JobExecutionAPIEntity jobExecutionAPIEntity = new JobExecutionAPIEntity();
        MRJob mrJob = mrJobs.get(0);
        jobExecutionAPIEntity.setTimestamp(app.getStartedTime());
        jobExecutionAPIEntity.setSubmissionTime(app.getStartedTime());
        jobExecutionAPIEntity.setTrackingUrl(app.getTrackingUrl());
        jobExecutionAPIEntity.setStartTime(mrJob.getStartTime());
        jobExecutionAPIEntity.setDurationTime(mrJob.getElapsedTime());
        jobExecutionAPIEntity.setCurrentState(mrJob.getState());
        jobExecutionAPIEntity.setInternalState(mrJob.getState());
        jobExecutionAPIEntity.setNumTotalMaps(mrJob.getMapsTotal());
        jobExecutionAPIEntity.setNumFinishedMaps(mrJob.getMapsCompleted());
        jobExecutionAPIEntity.setNumTotalReduces(mrJob.getReducesTotal());
        jobExecutionAPIEntity.setNumFinishedReduces(mrJob.getReducesCompleted());
        jobExecutionAPIEntity.setMapProgress(mrJob.getMapProgress());
        jobExecutionAPIEntity.setReduceProgress(mrJob.getReduceProgress());
        jobExecutionAPIEntity.setMapsPending(mrJob.getMapsPending());
        jobExecutionAPIEntity.setMapsRunning(mrJob.getMapsRunning());
        jobExecutionAPIEntity.setReducesPending(mrJob.getReducesPending());
        jobExecutionAPIEntity.setReducesRunning(mrJob.getReducesRunning());
        jobExecutionAPIEntity.setNewReduceAttempts(mrJob.getNewReduceAttempts());
        jobExecutionAPIEntity.setRunningReduceAttempts(mrJob.getRunningReduceAttempts());
        jobExecutionAPIEntity.setFailedReduceAttempts(mrJob.getFailedReduceAttempts());
        jobExecutionAPIEntity.setKilledReduceAttempts(mrJob.getKilledReduceAttempts());
        jobExecutionAPIEntity.setSuccessfulReduceAttempts(mrJob.getSuccessfulReduceAttempts());
        jobExecutionAPIEntity.setNewMapAttempts(mrJob.getNewMapAttempts());
        jobExecutionAPIEntity.setRunningMapAttempts(mrJob.getRunningMapAttempts());
        jobExecutionAPIEntity.setFailedMapAttempts(mrJob.getFailedMapAttempts());
        jobExecutionAPIEntity.setKilledMapAttempts(mrJob.getKilledMapAttempts());
        jobExecutionAPIEntity.setSuccessfulMapAttempts(mrJob.getSuccessfulMapAttempts());
        jobExecutionAPIEntity.setAppInfo(app);
        jobExecutionAPIEntity.setAllocatedMB(app.getAllocatedMB());
        jobExecutionAPIEntity.setAllocatedVCores(app.getAllocatedVCores());
        jobExecutionAPIEntity.setRunningContainers(app.getRunningContainers());
        return jobExecutionAPIEntity;
    }

    private JobCounters getJobCounters() {
        InputStream countstream = null;
        JobCounters jobCounters = null;
        try {
            countstream = this.getClass().getResourceAsStream("/jobcounts_30784.json");
            jobCounters = OBJ_MAPPER.readValue(countstream, JobCountersWrapper.class).getJobCounters();
        } catch (Exception e) {
            return null;
        } finally {
            if (countstream != null) {
                Utils.closeInputStream(countstream);
            }
        }
        return jobCounters;
    }

    private JobExecutionAPIEntity makeJobExecutionAPIEntityWithCounts() throws IOException {


        InputStream jsonstream = this.getClass().getResourceAsStream("/mrjob_30784.json");
        List<MRJob> mrJobs = OBJ_MAPPER.readValue(jsonstream, MRJobsWrapper.class).getJobs().getJob();

        InputStream previousmrrunningapp = this.getClass().getResourceAsStream("/previousmrrunningapp.json");
        AppsWrapper appsWrapper = OBJ_MAPPER.readValue(previousmrrunningapp, AppsWrapper.class);
        List<AppInfo> appInfos = appsWrapper.getApps().getApp();
        AppInfo app = appInfos.get(0);
        JobExecutionAPIEntity jobExecutionAPIEntity = new JobExecutionAPIEntity();
        MRJob mrJob = mrJobs.get(0);
        jobExecutionAPIEntity.setTimestamp(app.getStartedTime());
        jobExecutionAPIEntity.setSubmissionTime(app.getStartedTime());
        jobExecutionAPIEntity.setTrackingUrl(app.getTrackingUrl());
        jobExecutionAPIEntity.setStartTime(mrJob.getStartTime());
        jobExecutionAPIEntity.setDurationTime(mrJob.getElapsedTime());
        jobExecutionAPIEntity.setCurrentState(mrJob.getState());
        jobExecutionAPIEntity.setInternalState(mrJob.getState());
        jobExecutionAPIEntity.setNumTotalMaps(mrJob.getMapsTotal());
        jobExecutionAPIEntity.setNumFinishedMaps(mrJob.getMapsCompleted());
        jobExecutionAPIEntity.setNumTotalReduces(mrJob.getReducesTotal());
        jobExecutionAPIEntity.setNumFinishedReduces(mrJob.getReducesCompleted());
        jobExecutionAPIEntity.setMapProgress(mrJob.getMapProgress());
        jobExecutionAPIEntity.setReduceProgress(mrJob.getReduceProgress());
        jobExecutionAPIEntity.setMapsPending(mrJob.getMapsPending());
        jobExecutionAPIEntity.setMapsRunning(mrJob.getMapsRunning());
        jobExecutionAPIEntity.setReducesPending(mrJob.getReducesPending());
        jobExecutionAPIEntity.setReducesRunning(mrJob.getReducesRunning());
        jobExecutionAPIEntity.setNewReduceAttempts(mrJob.getNewReduceAttempts());
        jobExecutionAPIEntity.setRunningReduceAttempts(mrJob.getRunningReduceAttempts());
        jobExecutionAPIEntity.setFailedReduceAttempts(mrJob.getFailedReduceAttempts());
        jobExecutionAPIEntity.setKilledReduceAttempts(mrJob.getKilledReduceAttempts());
        jobExecutionAPIEntity.setSuccessfulReduceAttempts(mrJob.getSuccessfulReduceAttempts());
        jobExecutionAPIEntity.setNewMapAttempts(mrJob.getNewMapAttempts());
        jobExecutionAPIEntity.setRunningMapAttempts(mrJob.getRunningMapAttempts());
        jobExecutionAPIEntity.setFailedMapAttempts(mrJob.getFailedMapAttempts());
        jobExecutionAPIEntity.setKilledMapAttempts(mrJob.getKilledMapAttempts());
        jobExecutionAPIEntity.setSuccessfulMapAttempts(mrJob.getSuccessfulMapAttempts());
        jobExecutionAPIEntity.setAppInfo(app);
        jobExecutionAPIEntity.setAllocatedMB(app.getAllocatedMB());
        jobExecutionAPIEntity.setAllocatedVCores(app.getAllocatedVCores());
        jobExecutionAPIEntity.setRunningContainers(app.getRunningContainers());

        applyJobCounts(jobExecutionAPIEntity);

        return jobExecutionAPIEntity;
    }

    private void applyJobCounts(JobExecutionAPIEntity jobExecutionAPIEntity) {

        JobCounters jobCounters = getJobCounters();
        org.apache.eagle.jpm.util.jobcounter.JobCounters jobCounter = new org.apache.eagle.jpm.util.jobcounter.JobCounters();
        Map<String, Map<String, Long>> groups = new HashMap<>();

        for (JobCounterGroup jobCounterGroup : jobCounters.getCounterGroup()) {
            String counterGroupName = jobCounterGroup.getCounterGroupName();
            if (!groups.containsKey(counterGroupName)) {
                groups.put(counterGroupName, new HashMap<>());
            }

            Map<String, Long> counterValues = groups.get(counterGroupName);
            List<JobCounterItem> items = jobCounterGroup.getCounter();
            if (items == null) {
                continue;
            }
            for (JobCounterItem item : items) {
                String key = item.getName();
                counterValues.put(key, item.getTotalCounterValue());
                if (counterGroupName.equals(Constants.JOB_COUNTER)) {
                    if (key.equals(Constants.JobCounter.DATA_LOCAL_MAPS.toString())) {
                        jobExecutionAPIEntity.setDataLocalMaps((int) item.getTotalCounterValue());
                    } else if (key.equals(Constants.JobCounter.RACK_LOCAL_MAPS.toString())) {
                        jobExecutionAPIEntity.setRackLocalMaps((int) item.getTotalCounterValue());
                    } else if (key.equals(Constants.JobCounter.TOTAL_LAUNCHED_MAPS.toString())) {
                        jobExecutionAPIEntity.setTotalLaunchedMaps((int) item.getTotalCounterValue());
                    }
                }
            }
        }

        jobCounter.setCounters(groups);
        jobExecutionAPIEntity.setJobCounters(jobCounter);
        if (jobExecutionAPIEntity.getTotalLaunchedMaps() > 0) {
            jobExecutionAPIEntity.setDataLocalMapsPercentage(jobExecutionAPIEntity.getDataLocalMaps() * 1.0 / jobExecutionAPIEntity.getTotalLaunchedMaps());
            jobExecutionAPIEntity.setRackLocalMapsPercentage(jobExecutionAPIEntity.getRackLocalMaps() * 1.0 / jobExecutionAPIEntity.getTotalLaunchedMaps());
        }
    }
}
