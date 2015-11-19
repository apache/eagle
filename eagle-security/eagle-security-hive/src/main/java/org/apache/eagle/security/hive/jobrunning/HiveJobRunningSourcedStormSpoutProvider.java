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

import backtype.storm.topology.base.BaseRichSpout;
import org.apache.eagle.job.JobPartitioner;
import org.apache.eagle.jobrunning.config.RunningJobCrawlConfig;
import org.apache.eagle.jobrunning.config.RunningJobCrawlConfig.ControlConfig;
import org.apache.eagle.jobrunning.config.RunningJobCrawlConfig.RunningJobEndpointConfig;
import org.apache.eagle.jobrunning.config.RunningJobCrawlConfig.ZKStateConfig;
import org.apache.eagle.jobrunning.storm.JobRunningSpout;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveJobRunningSourcedStormSpoutProvider {
	private static final Logger LOG = LoggerFactory.getLogger(HiveJobRunningSourcedStormSpoutProvider.class);
	
	public BaseRichSpout getSpout(Config config, int parallelism){
		RunningJobEndpointConfig endPointConfig = new RunningJobEndpointConfig();
		String RMEndPoints = config.getString("dataSourceConfig.RMEndPoints");				
		endPointConfig.RMBasePaths = RMEndPoints.split(",");
		
		String HSEndPoint = config.getString("dataSourceConfig.HSEndPoint");
		endPointConfig.HSBasePath = HSEndPoint;
		
		ControlConfig controlConfig = new ControlConfig();
		controlConfig.jobInfoEnabled = true;
		controlConfig.jobConfigEnabled = true;
        controlConfig.numTotalPartitions = parallelism <= 0 ? 1 : parallelism;
        
        boolean zkCleanupTimeSet = config.hasPath("dataSourceConfig.zkCleanupTimeInday");
        //default set as two days
        controlConfig.zkCleanupTimeInday = zkCleanupTimeSet ? config.getInt("dataSourceConfig.zkCleanupTimeInday") : 2;
        
        boolean completedJobOutofDateTimeSet = config.hasPath("dataSourceConfig.completedJobOutofDateTimeInMin");
        controlConfig.completedJobOutofDateTimeInMin = completedJobOutofDateTimeSet ? config.getInt("dataSourceConfig.completedJobOutofDateTimeInMin") : 120;
        
        boolean sizeOfJobConfigQueueSet = config.hasPath("dataSourceConfig.sizeOfJobConfigQueue");
        controlConfig.sizeOfJobConfigQueue = sizeOfJobConfigQueueSet ? config.getInt("dataSourceConfig.sizeOfJobConfigQueue") : 10000;

        boolean sizeOfJobCompletedInfoQueue = config.hasPath("dataSourceConfig.sizeOfJobCompletedInfoQueue");
        controlConfig.sizeOfJobCompletedInfoQueue = sizeOfJobCompletedInfoQueue ? config.getInt("dataSourceConfig.sizeOfJobCompletedInfoQueue") : 10000;
        
        //controlConfig.numTotalPartitions = parallelism == null ? 1 : parallelism;        
		ZKStateConfig zkStateConfig = new ZKStateConfig();
		zkStateConfig.zkQuorum = config.getString("dataSourceConfig.zkQuorum");
		zkStateConfig.zkRoot = config.getString("dataSourceConfig.zkRoot");
		zkStateConfig.zkSessionTimeoutMs = config.getInt("dataSourceConfig.zkSessionTimeoutMs");
		zkStateConfig.zkRetryTimes = config.getInt("dataSourceConfig.zkRetryTimes");
		zkStateConfig.zkRetryInterval = config.getInt("dataSourceConfig.zkRetryInterval");
		RunningJobCrawlConfig crawlConfig = new RunningJobCrawlConfig(endPointConfig, controlConfig, zkStateConfig);

		try{
			controlConfig.partitionerCls = (Class<? extends JobPartitioner>)Class.forName(config.getString("dataSourceConfig.partitionerCls"));
		}
		catch(Exception ex){
			LOG.error("failing find job id partitioner class " + config.getString("dataSourceConfig.partitionerCls"));
			throw new IllegalStateException("jobId partitioner class does not exist " + config.getString("dataSourceConfig.partitionerCls"));
		}
		
		JobRunningSpout spout = new JobRunningSpout(crawlConfig);
		return spout;
	}
}
