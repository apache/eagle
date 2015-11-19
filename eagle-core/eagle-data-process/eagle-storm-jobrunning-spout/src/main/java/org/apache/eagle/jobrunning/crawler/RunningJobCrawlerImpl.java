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
package org.apache.eagle.jobrunning.crawler;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.eagle.jobrunning.zkres.JobRunningZKStateManager;
import org.apache.commons.lang.time.DateUtils;
import org.apache.eagle.jobrunning.config.RunningJobCrawlConfig;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.job.JobFilter;
import org.apache.eagle.jobrunning.callback.RunningJobCallback;
import org.apache.eagle.jobrunning.common.JobConstants.JobState;
import org.apache.eagle.jobrunning.common.JobConstants.ResourceType;
import org.apache.eagle.jobrunning.common.JobConstants.YarnApplicationType;
import org.apache.eagle.jobrunning.util.JobUtils;
import org.apache.eagle.jobrunning.yarn.model.AppInfo;
import org.apache.eagle.common.DateTimeUtil;

public class RunningJobCrawlerImpl implements RunningJobCrawler{

	protected RunningJobCrawlConfig.RunningJobEndpointConfig endpointConfig;
	protected RunningJobCrawlConfig.ControlConfig controlConfig;
	protected JobFilter jobFilter;
	private ResourceFetcher fetcher;
	private JobRunningZKStateManager zkStateManager;
	private Thread jobConfigProcessThread;
	private Thread jobCompleteInfoProcessThread;
	private Thread jobCompleteStatusCheckerThread;
	private Thread zkCleanupThread;
	private final RunningJobCallback callback;
	private ReadWriteLock readWriteLock;
	private Map<ResourceType, Map<String, JobContext>> processingJobMap = new ConcurrentHashMap<ResourceType, Map<String, JobContext>>();
	
	private BlockingQueue<JobContext> queueOfConfig;
	private BlockingQueue<JobContext> queueOfCompleteJobInfo;
	private static final int DEFAULT_CONFIG_THREAD_COUNT = 20;
	private final long DELAY_TO_UPDATE_COMPLETION_JOB_INFO = 5 * DateUtils.MILLIS_PER_MINUTE;
	private static final Logger LOG = LoggerFactory.getLogger(RunningJobCrawlerImpl.class);
	
	private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

	static {
		OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
	}
	
	public RunningJobCrawlerImpl(RunningJobCrawlConfig config, JobRunningZKStateManager zkStateManager, 
			RunningJobCallback callback, JobFilter jobFilter, ReadWriteLock readWriteLock) {
		this.endpointConfig = config.endPointConfig;
		this.controlConfig = config.controlConfig;
		this.callback = callback;
		this.fetcher = new RMResourceFetcher(endpointConfig);
		this.jobFilter = jobFilter;
		this.readWriteLock = readWriteLock;
		if (config.controlConfig.jobInfoEnabled) {
			jobCompleteInfoProcessThread = new Thread() {
				@Override
				public void run() {
					startCompleteJobInfoProcessThread();
				}
			};
			jobCompleteInfoProcessThread.setName("JobCompleteInfo-process-thread");
			jobCompleteInfoProcessThread.setDaemon(true);
			
			jobCompleteStatusCheckerThread = new Thread() {
				@Override
				public void run() {
					startCompleteStatusCheckerThread();
				}
			};
			jobCompleteStatusCheckerThread.setName("JobComplete-statusChecker-thread");
			jobCompleteStatusCheckerThread.setDaemon(true);
		}

		if (config.controlConfig.jobConfigEnabled) {
			jobConfigProcessThread = new Thread() {
				@Override
				public void run() {
					startJobConfigProcessThread();
				}
			};
			jobConfigProcessThread.setName("JobConfig-process-thread");
			jobConfigProcessThread.setDaemon(true);
		}
				
		zkCleanupThread = new Thread() {
			@Override
			public void run() {
				startzkCleanupThread();
			}
		};		
		zkCleanupThread.setName("zk-cleanup-thread");
		zkCleanupThread.setDaemon(true);
		
		this.zkStateManager = zkStateManager;
		this.processingJobMap.put(ResourceType.JOB_CONFIGURATION, new ConcurrentHashMap<String, JobContext>());
		this.processingJobMap.put(ResourceType.JOB_COMPLETE_INFO, new ConcurrentHashMap<String, JobContext>());
		this.queueOfConfig = new ArrayBlockingQueue<JobContext>(controlConfig.sizeOfJobConfigQueue);
		this.queueOfCompleteJobInfo = new ArrayBlockingQueue<JobContext>(controlConfig.sizeOfJobCompletedInfoQueue);
	}
	
	private void startJobConfigProcessThread() {
		int configThreadCount = DEFAULT_CONFIG_THREAD_COUNT;
		LOG.info("Job Config crawler main thread started, pool size: " + DEFAULT_CONFIG_THREAD_COUNT);

    	ThreadFactory factory = new ThreadFactory() {
			private final AtomicInteger count = new AtomicInteger(0);

			public Thread newThread(Runnable runnable) {
				count.incrementAndGet();
				Thread thread = Executors.defaultThreadFactory().newThread(runnable);
				thread.setName("config-crawler-workthread-" + count.get());
				return thread;
			}
		};
		
		ThreadPoolExecutor pool = new ThreadPoolExecutor(configThreadCount, configThreadCount, 0L,
									  TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), factory);
		
		while (true) {
			JobContext context;
			try {
				context = queueOfConfig.take();
				LOG.info("queueOfConfig size: " + queueOfConfig.size());
				Runnable configCrawlerThread = new ConfigWorkTask(new JobContext(context), fetcher, callback, this);
				pool.execute(configCrawlerThread);
			} catch (InterruptedException e) {
				LOG.warn("Got an InterruptedException: " + e.getMessage());
			} catch (RejectedExecutionException e2) {
				LOG.warn("Got RejectedExecutionException: " + e2.getMessage());
			}
			catch (Throwable t) {
				LOG.warn("Got an throwable t, " + t.getMessage());
			}
		}
	}
	
	private void startCompleteJobInfoProcessThread() {
		while(true) {
			JobContext context = null;
			try {
				context = queueOfCompleteJobInfo.take();
			} catch (InterruptedException ex) {
			}
			/** Delay an interval before fetch job complete info, for history url need some time to be accessible,
			 *  The default interval is set as 5 min,
			 *  Also need to consider if need multi thread to do this
			 */
			while (System.currentTimeMillis() < 
			      context.fetchedTime + DELAY_TO_UPDATE_COMPLETION_JOB_INFO) {
				try {
					Thread.sleep(50);
				} catch (InterruptedException e1) {
				}
			}
			try {
				List<Object> objs = fetcher.getResource(ResourceType.JOB_COMPLETE_INFO, JobUtils.getAppIDByJobID(context.jobId));
				callback.onJobRunningInformation(context, ResourceType.JOB_COMPLETE_INFO, objs);
			}
			catch(Exception ex) {
		        if (ex.getMessage().contains("Server returned HTTP response code: 500")) {
		        	LOG.warn("The server returns 500 error, it's probably caused by job ACL setting, going to skip this job");
		        	// the job remains in processing list, thus we will not do infructuous retry next round
		        	// TODO need remove it from processing list when job finished to avoid memory leak 
		        }
		        else LOG.error("Got an exception when fetching resource ", ex);
			}
		}
	}
	
	public void startCompleteStatusCheckerThread() {
		while(true) {
			List<Object> list;
			try {
				list = fetcher.getResource(ResourceType.JOB_LIST, JobState.COMPLETED.name());
				if (list == null) {
					LOG.warn("Current Completed Job List is Empty");
					continue;
				}
				@SuppressWarnings("unchecked")
				List<AppInfo> apps = (List<AppInfo>)list.get(0);
				Set<JobContext> completedJobSet = new HashSet<JobContext>();
				for (AppInfo app : apps) {
					//Only fetch MapReduce job
					if (!YarnApplicationType.MAPREDUCE.name().equals(app.getApplicationType()) 
					|| !jobFilter.accept(app.getUser())) {
						continue;
					}
					if (System.currentTimeMillis() - app.getFinishedTime() < controlConfig.completedJobOutofDateTimeInMin * DateUtils.MILLIS_PER_MINUTE) {
						completedJobSet.add(new JobContext(JobUtils.getJobIDByAppID(app.getId()),app.getUser(), System.currentTimeMillis()));
					}
				}
				
				if (controlConfig.jobConfigEnabled) {
					addIntoProcessingQueueAndList(completedJobSet, queueOfConfig, ResourceType.JOB_CONFIGURATION);
				}

				if (controlConfig.jobInfoEnabled) {
					addIntoProcessingQueueAndList(completedJobSet, queueOfCompleteJobInfo, ResourceType.JOB_COMPLETE_INFO);
				}
				Thread.sleep(20 * 1000);
			} catch (Throwable t) {
				LOG.error("Got a throwable in fetching job completed list :", t);
			}						
		}
	}
	
	public void startzkCleanupThread() {
		LOG.info("zk cleanup thread started");
		while(true) {
			try {
				long thresholdTime = System.currentTimeMillis() - controlConfig.zkCleanupTimeInday * DateUtils.MILLIS_PER_DAY; 
				String date = DateTimeUtil.format(thresholdTime, "yyyyMMdd");
				zkStateManager.truncateJobBefore(ResourceType.JOB_CONFIGURATION, date);
				zkStateManager.truncateJobBefore(ResourceType.JOB_COMPLETE_INFO, date);
				Thread.sleep(30 * 60 * 1000);
			}
			catch (Throwable t) {
				LOG.warn("Got an throwable, t: ", t);
			}
		}
	}
	
	public void addIntoProcessingQueueAndList(Set<JobContext> jobSet, BlockingQueue<JobContext> queue, ResourceType type) {
		try {
			readWriteLock.writeLock().lock();
			LOG.info("Write lock acquired");
			List<String> processingList = zkStateManager.readProcessedJobs(type);
			processingList.addAll(extractJobList(type));
			for (JobContext context: jobSet) {
				String jobId = context.jobId;
				if (!processingList.contains(jobId)) {
					addIntoProcessingList(type, context);
					queue.add(context);
				}
			}
		}
		finally {
			try {readWriteLock.writeLock().unlock(); LOG.info("Write lock released");}
			catch (Throwable t) { LOG.error("Fail to release Write lock", t);}
		}
	}
	
	private List<String> extractJobList(ResourceType type) {
		Map<String, JobContext> contexts = processingJobMap.get(type);
		return Arrays.asList(contexts.keySet().toArray(new String[0]));
	}
	
	@Override
	public void crawl() throws Exception {
		// bring up crawler threads when crawl method is invoked first time
		if (jobConfigProcessThread != null && !jobConfigProcessThread.isAlive()) {
			jobConfigProcessThread.start();
		}
		
		if (jobCompleteInfoProcessThread != null && !jobCompleteInfoProcessThread.isAlive()) {
			jobCompleteInfoProcessThread.start();
		}		
 
		if (jobCompleteStatusCheckerThread != null && !jobCompleteStatusCheckerThread.isAlive()) {
			jobCompleteStatusCheckerThread.start();
		}
		
		if (!zkCleanupThread.isAlive()) {
			zkCleanupThread.start();
		}
		
		List<Object> list = fetcher.getResource(ResourceType.JOB_LIST, JobState.RUNNING.name());		
		if (list == null) {
			LOG.warn("Current Running Job List is Empty");
			return;
		}
		
		@SuppressWarnings("unchecked")
		List<AppInfo> apps = (List<AppInfo>)list.get(0);
		LOG.info("Current Running Job List size : " + apps.size());
		Set<JobContext> currentRunningJobSet = new HashSet<JobContext>();
		for (AppInfo app : apps) {
			//Only fetch MapReduce job
			if (!YarnApplicationType.MAPREDUCE.name().equals(app.getApplicationType()) 
			|| !jobFilter.accept(app.getUser())) {
				continue;
			}
			currentRunningJobSet.add(new JobContext(JobUtils.getJobIDByAppID(app.getId()), app.getUser(), System.currentTimeMillis()));
		}
		
		if (controlConfig.jobConfigEnabled) {
			addIntoProcessingQueueAndList(currentRunningJobSet, queueOfConfig, ResourceType.JOB_CONFIGURATION);
		}
		
		if (controlConfig.jobInfoEnabled) {			
			// fetch job detail & jobcounters	
			for (JobContext context : currentRunningJobSet) {
				try {
					List<Object> objs = fetcher.getResource(ResourceType.JOB_RUNNING_INFO, JobUtils.getAppIDByJobID(context.jobId));
					callback.onJobRunningInformation(context, ResourceType.JOB_RUNNING_INFO, objs);
				}
				catch (Exception ex) {
			        if (ex.getMessage().contains("Server returned HTTP response code: 500")) {
			        	LOG.warn("The server returns 500 error, it's probably caused by job ACL setting, going to skip this job");
			        	// the job remains in processing list, thus we will not do infructuous retry next round
			        	// TODO need remove it from processing list when job finished to avoid memory leak 
			        }
			        else LOG.error("Got an exception when fetching resource, jobId: " + context.jobId , ex);
				}
			}
		}		
	}

	@Override
	public void addIntoProcessingList(ResourceType type, JobContext context) {
		processingJobMap.get(type).put(context.jobId, context);
	}

	@Override
	public void removeFromProcessingList(ResourceType type, JobContext context) {
		processingJobMap.get(type).remove(context.jobId);
	}
}
