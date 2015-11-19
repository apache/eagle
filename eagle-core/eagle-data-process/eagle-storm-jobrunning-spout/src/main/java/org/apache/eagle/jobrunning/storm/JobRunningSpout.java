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
package org.apache.eagle.jobrunning.storm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.eagle.jobrunning.callback.DefaultRunningJobInputStreamCallback;
import org.apache.eagle.jobrunning.callback.RunningJobMessageId;
import org.apache.eagle.jobrunning.config.RunningJobCrawlConfig;
import org.apache.eagle.jobrunning.crawler.RunningJobCrawler;
import org.apache.eagle.jobrunning.crawler.RunningJobCrawlerImpl;
import org.apache.eagle.jobrunning.zkres.JobRunningZKStateManager;
import org.apache.eagle.jobrunning.common.JobConstants;
import org.apache.eagle.jobrunning.crawler.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import org.apache.eagle.job.JobFilter;
import org.apache.eagle.job.JobFilterByPartition;
import org.apache.eagle.job.JobPartitioner;
import org.apache.eagle.jobrunning.callback.RunningJobCallback;

public class JobRunningSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(JobRunningSpout.class);

	private RunningJobCrawlConfig config;
	private JobRunningZKStateManager zkStateManager;
	private transient RunningJobCrawler crawler;
	private JobRunningSpoutCollectorInterceptor interceptor;
	private RunningJobCallback callback;
	private ReadWriteLock readWriteLock;
    private static final int DEFAULT_WAIT_SECONDS_BETWEEN_ROUNDS = 10;

    public JobRunningSpout(RunningJobCrawlConfig config){
		this(config, new JobRunningSpoutCollectorInterceptor());
	}
	
	/**
	 * mostly this constructor signature is for unit test purpose as you can put customized interceptor here
	 * @param config
	 * @param interceptor
	 */
	public JobRunningSpout(RunningJobCrawlConfig config, JobRunningSpoutCollectorInterceptor interceptor){
		this.config = config;
		this.interceptor = interceptor;
		this.callback = new DefaultRunningJobInputStreamCallback(interceptor);
		this.readWriteLock = new ReentrantReadWriteLock();
	}
	
	
	/**
	 * TODO: just copy this part from jobHistorySpout, need to move it to a common place
	 * @param context
	 * @return
	 */
	private int calculatePartitionId(TopologyContext context){
		int thisGlobalTaskId = context.getThisTaskId();
		String componentName = context.getComponentId(thisGlobalTaskId);
		List<Integer> globalTaskIds = context.getComponentTasks(componentName);
		int index = 0;
		for(Integer id : globalTaskIds){
			if(id == thisGlobalTaskId){
				return index;
			}
			index++;
		}
		throw new IllegalStateException();
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		int partitionId = calculatePartitionId(context);
		// sanity verify 0<=partitionId<=numTotalPartitions-1
		if(partitionId < 0 || partitionId > config.controlConfig.numTotalPartitions){
			throw new IllegalStateException("partitionId should be less than numTotalPartitions with partitionId " + 
					partitionId + " and numTotalPartitions " + config.controlConfig.numTotalPartitions);
		}
		Class<? extends JobPartitioner> partitionerCls = config.controlConfig.partitionerCls;
		JobPartitioner partitioner = null;
		try {
			partitioner = partitionerCls.newInstance();
		} catch (Exception e) {
			LOG.error("failing instantiating job partitioner class " + partitionerCls.getCanonicalName());
			throw new IllegalStateException(e);
		}
		JobFilter jobFilter = new JobFilterByPartition(partitioner, config.controlConfig.numTotalPartitions, partitionId);
		interceptor.setSpoutOutputCollector(collector);		
		try {
			zkStateManager = new JobRunningZKStateManager(config);
			crawler = new RunningJobCrawlerImpl(config, zkStateManager, callback, jobFilter, readWriteLock);
		} catch (Exception e) {
			LOG.error("failing creating crawler driver");
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void nextTuple() {
		try{
			crawler.crawl();
		}catch(Exception ex){
			LOG.error("fail crawling running job and continue ...", ex);
		}
        try{
            Thread.sleep(DEFAULT_WAIT_SECONDS_BETWEEN_ROUNDS *1000);
        }catch(Exception x){
        }
    }
	
	/**
	 * empty because framework will take care of output fields declaration
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
	/**
	 * add to processedJob
	 */
	@Override
    public void ack(Object msgId) {
		RunningJobMessageId messageId = (RunningJobMessageId) msgId;
		JobConstants.ResourceType type = messageId.type;
		LOG.info("Ack on messageId: " + messageId.toString());
		switch(type) {
			case JOB_CONFIGURATION:
			case JOB_COMPLETE_INFO:
				/** lock this for making processed/processing job list unchanged during crawler calculating last round running job list **/
				try {
					readWriteLock.readLock().lock();
					zkStateManager.addProcessedJob(type, messageId.jobID);
					// Here username & timestamp is meaningless, set to null
					crawler.removeFromProcessingList(type, new JobContext(messageId.jobID, null, null));
				}
				finally {
					try {readWriteLock.readLock().unlock(); LOG.info("Read lock released");}
					catch (Throwable t) { LOG.error("Fail to release Read lock", t);}
				}
				break;				
			default:
				break;
		}	
    }

	/**
	 * job is not fully processed
	 */
    @Override
    public void fail(Object msgId) {
		RunningJobMessageId messageId = (RunningJobMessageId) msgId;
		JobConstants.ResourceType type = messageId.type;
		// Here timestamp is meaningless, set to null
		if (type.equals(JobConstants.ResourceType.JOB_COMPLETE_INFO) || type.equals(JobConstants.ResourceType.JOB_CONFIGURATION)) {
			try {
				readWriteLock.readLock().lock();
				// Here username in not used, set to null
				crawler.removeFromProcessingList(type, new JobContext(messageId.jobID, null, null));
			}
			finally {
				try {readWriteLock.readLock().unlock(); LOG.info("Read lock released");}
				catch (Throwable t) { LOG.error("Fail to release Read lock", t);}
			}
		}
    }
   
    @Override
    public void deactivate() {
    	
    }
   
    @Override
    public void close() {
    	
    }
}
