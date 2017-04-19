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

package org.apache.eagle.jpm.mr.history.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import org.apache.eagle.jpm.mr.history.MRHistoryJobConfig;
import org.apache.eagle.jpm.mr.history.crawler.*;
import org.apache.eagle.jpm.mr.history.publisher.StreamPublisher;
import org.apache.eagle.jpm.mr.history.publisher.StreamPublisherManager;
import org.apache.eagle.jpm.mr.history.zkres.JobHistoryZKStateManager;
import org.apache.eagle.jpm.mr.historyentity.JobProcessTimeStampEntity;
import org.apache.eagle.jpm.util.DefaultJobIdPartitioner;
import org.apache.eagle.jpm.util.JobIdFilter;
import org.apache.eagle.jpm.util.JobIdFilterByPartition;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.eagle.jpm.mr.history.MRHistoryJobConfig.JobHistoryEndpointConfig;


/**
 * Zookeeper znode structure
 * -zkRoot
 * - partitions
 * - 0 (20150101)
 * - 1 (20150101)
 * - 2 (20150101)
 * - ... ...
 * - N-1 (20150102)
 * - jobs
 * - 20150101
 * - job1
 * - job2
 * - job3
 * - 20150102
 * - job1
 * - job2
 * - job3
 * <p>
 * Spout can have multiple instances, which is supported by storm parallelism primitive.
 * </p>
 * <p>
 * Under znode partitions, N child znodes (name is 0 based integer) would be created with each znode mapped to one spout instance. All jobs will be partitioned into N
 * partitions by applying JobPartitioner class to each job Id. The value of each partition znode is the date when the last job in this partition
 * is successfully processed.
 * </p>
 * <p>
 * processing steps
 * 1) In constructor,
 * 2) In open(), calculate jobPartitionId for current spout (which should be exactly same to spout taskId within TopologyContext)
 * 3) In open(), zkState.ensureJobPartitions to rebuild znode partitions if necessary. ensureJobPartitions is only done by one spout task as internally this is using lock
 * 5) In nextTuple(), list job files by invoking hadoop API
 * 6) In nextTuple(), iterate each jobId and invoke JobPartition.partition(jobId) and keep those jobs belonging to current partition Id
 * 7) process job files (job history file and job configuration xml file)
 * 8) add job Id to current date slot say for example 20150102 after this job is successfully processed
 * 9) clean up all slots with date less than currentProcessDate - 2 days. (2 days should be configurable)
 * </p>
 * Note:
 * if one spout instance crashes and is brought up again, open() method would be invoked again, we need think of this scenario.
 */

public class JobHistorySpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(JobHistorySpout.class);

    private int partitionId;
    private int numTotalPartitions;
    private transient JHFCrawlerDriver driver;
    private JobHistoryContentFilter contentFilter;
    private JobHistorySpoutCollectorInterceptor interceptor;
    private JHFInputStreamCallback callback;
    private JobHistoryLCM jhfLCM;
    private static final int MAX_RETRY_TIMES = 3;
    private MRHistoryJobConfig appConfig;
    private JobHistoryEndpointConfig jobHistoryEndpointConfig;
    private List<StreamPublisher> streamPublishers;

    /**
     * mostly this constructor signature is for unit test purpose as you can put customized interceptor here.
     */
    public JobHistorySpout(JobHistoryContentFilter filter, MRHistoryJobConfig appConfig) {
        this.contentFilter = filter;
        this.interceptor = new JobHistorySpoutCollectorInterceptor();
        this.appConfig = appConfig;
        jobHistoryEndpointConfig = appConfig.getJobHistoryEndpointConfig();
        callback = new DefaultJHFInputStreamCallback(contentFilter, appConfig);
    }

    private int calculatePartitionId(TopologyContext context) {
        int thisGlobalTaskId = context.getThisTaskId();
        String componentName = context.getComponentId(thisGlobalTaskId);
        List<Integer> globalTaskIds = context.getComponentTasks(componentName);
        numTotalPartitions = globalTaskIds.size();
        int index = 0;
        for (Integer id : globalTaskIds) {
            if (id == thisGlobalTaskId) {
                return index;
            }
            index++;
        }
        throw new IllegalStateException();
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     final SpoutOutputCollector collector) {
        partitionId = calculatePartitionId(context);
        // sanity verify 0<=partitionId<=numTotalPartitions-1
        if (partitionId < 0 || partitionId > numTotalPartitions) {
            throw new IllegalStateException("partitionId should be less than numTotalPartitions with partitionId "
                + partitionId + " and numTotalPartitions " + numTotalPartitions);
        }
        JobIdFilter jobIdFilter = new JobIdFilterByPartition(new DefaultJobIdPartitioner(), numTotalPartitions, partitionId);
        JobHistoryZKStateManager.instance().init(appConfig.getZkStateConfig());
        JobHistoryZKStateManager.instance().ensureJobPartition(partitionId, numTotalPartitions);
        interceptor.setSpoutOutputCollector(collector);
        if (streamPublishers != null) {
            for (StreamPublisher streamPublisher : streamPublishers) {
                streamPublisher.setCollector(this.interceptor);
                StreamPublisherManager.getInstance().addStreamPublisher(streamPublisher);
            }
        }
        try {
            jhfLCM = new JobHistoryDAOImpl(jobHistoryEndpointConfig);
            driver = new JHFCrawlerDriverImpl(
                callback,
                jhfLCM,
                jobIdFilter,
                partitionId,
                appConfig);
        } catch (Exception e) {
            LOG.error("failing creating crawler driver");
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            Long modifiedTime = driver.crawl();
            JobHistoryZKStateManager.instance().updateProcessedTimeStamp(partitionId, modifiedTime);
            updateProcessedTimeStamp(modifiedTime);
        } catch (Exception ex) {
            LOG.error("fail crawling job history file and continue ...", ex);
            try {
                jhfLCM.freshFileSystem();
            } catch (Exception e) {
                LOG.error("failed to fresh file system ", e);
            }
        } finally {
            try {
                Thread.sleep(5000);
            } catch (Exception e) {
                // ignored
            }
        }
    }

    public void setStreamPublishers(List<StreamPublisher> streamPublishers) {
        this.streamPublishers = streamPublishers;
    }

    /**
     * empty because framework will take care of output fields declaration.
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (streamPublishers != null) {
            for (StreamPublisher streamPublisher : streamPublishers) {
                declarer.declareStream(streamPublisher.stormStreamId(), new Fields("f1", "message"));
            }
        } else {
            declarer.declare(new Fields("f1", "message"));
        }
    }

    /**
     * add to processedJob.
     */
    @Override
    public void ack(Object jobId) {
    }

    /**
     * job is not fully processed.
     */
    @Override
    public void fail(Object jobId) {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void close() {
    }

    private void updateProcessedTimeStamp(long modifiedTime) {
        if (partitionId != 0) {
            return;
        }

        //update latest process time
        long minTimeStamp = modifiedTime;
        for (int i = 1; i < numTotalPartitions; i++) {
            long time = JobHistoryZKStateManager.instance().readProcessedTimeStamp(i);
            if (time <= minTimeStamp) {
                minTimeStamp = time;
            }
        }

        if (minTimeStamp == 0L) {
            return;
        }

        LOG.info("updating the latest updated process time {}", minTimeStamp);
        Map<String, String> baseTags = new HashMap<String, String>() {
            {
                put("site", jobHistoryEndpointConfig.site);
            }
        };
        JobProcessTimeStampEntity entity = new JobProcessTimeStampEntity();
        entity.setCurrentTimeStamp(minTimeStamp);
        entity.setTimestamp(minTimeStamp);
        entity.setTags(baseTags);

        MRHistoryJobConfig.EagleServiceConfig eagleServiceConfig = appConfig.getEagleServiceConfig();
        IEagleServiceClient client = new EagleServiceClientImpl(
                eagleServiceConfig.eagleServiceHost,
                eagleServiceConfig.eagleServicePort,
                eagleServiceConfig.username,
                eagleServiceConfig.password);

        client.getJerseyClient().setReadTimeout(eagleServiceConfig.readTimeoutSeconds * 1000);

        List<JobProcessTimeStampEntity> entities = new ArrayList<>();
        entities.add(entity);

        int tried = 0;
        while (tried <= MAX_RETRY_TIMES) {
            try {
                LOG.info("start flushing {} JobProcessTimeStampEntity entities", entities.size());
                client.create(entities);
                LOG.info("finish flushing entities of total number " + entities.size());
                break;
            } catch (Exception ex) {
                if (tried < MAX_RETRY_TIMES) {
                    LOG.error("Got exception to flush, retry as " + (tried + 1) + " times", ex);
                } else {
                    LOG.error("Got exception to flush, reach max retry times " + MAX_RETRY_TIMES, ex);
                }
            }
            tried++;
        }

        client.getJerseyClient().destroy();
        try {
            client.close();
        } catch (Exception e) {
            LOG.error("failed to close eagle service client ", e);
        }
    }
}
