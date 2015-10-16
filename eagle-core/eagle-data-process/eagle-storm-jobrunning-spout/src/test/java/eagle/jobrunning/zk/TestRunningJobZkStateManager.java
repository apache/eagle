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
package eagle.jobrunning.zk;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import eagle.common.config.EagleConfigFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Test;

import eagle.jobrunning.common.JobConstants.ResourceType;
import eagle.jobrunning.config.RunningJobCrawlConfig;
import eagle.jobrunning.config.RunningJobCrawlConfig.ZKStateConfig;
import eagle.jobrunning.zkres.JobRunningZKStateManager;
import eagle.common.DateTimeUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestRunningJobZkStateManager {

	@Test
	public void testZKManager() throws Exception {
		System.setProperty("config.resource","/hive-jobrunning.conf");
		Config config = ConfigFactory.load();
		ZKStateConfig zkStateConfig = new ZKStateConfig();
		zkStateConfig.zkQuorum = config.getString("dataSourceConfig.zkQuorum");
		zkStateConfig.zkRoot = config.getString("dataSourceConfig.zkRoot");
		zkStateConfig.zkSessionTimeoutMs = config.getInt("dataSourceConfig.zkSessionTimeoutMs");
		zkStateConfig.zkRetryTimes = config.getInt("dataSourceConfig.zkRetryTimes");
		zkStateConfig.zkRetryInterval = config.getInt("dataSourceConfig.zkRetryInterval");

		RunningJobCrawlConfig crawlConfig = new RunningJobCrawlConfig(null, null, zkStateConfig);
		JobRunningZKStateManager manager = new JobRunningZKStateManager(crawlConfig);

		List<String> jobList = manager.readProcessedJobs(ResourceType.JOB_CONFIGURATION);
		
		CuratorFramework curator =  CuratorFrameworkFactory.newClient(
            	"localhost:12181",
                15000,
                15000,
                new RetryNTimes(3, 1000)
            );
		curator.start();
		System.out.println(jobList.size());
		for (String job : jobList) {
			byte[] bytes = curator.getData().forPath("/jobrunning/JOB_CONFIGURATION/jobs/" + job);
			String date = new String(bytes, StandardCharsets.UTF_8);
			if (date.contains(".")) {
				System.out.println(date);
			}
		}
	}
	
	@Test
	public void testCuratorJobDate() throws Exception{
		String date = DateTimeUtil.format(System.currentTimeMillis(), "yyyyMMdd");
		System.out.println(date);
		CuratorFramework curator =  CuratorFrameworkFactory.newClient(
								            	"localhost:12181",
								                15000,
								                15000,
								                new RetryNTimes(3, 1000)
								            );
		curator.start();
		String path = "/jobrunning/JOB_CONFIGURATION/jobs/job_1437549935027_553208";
		if (curator.checkExists().forPath(path) == null) {
			curator.create().creatingParentsIfNeeded().forPath(path, date.getBytes(StandardCharsets.UTF_8));
		}
        byte[] bytes = curator.getData().forPath(path);
        String dateStr = new String(bytes, StandardCharsets.UTF_8);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        sdf.setTimeZone(EagleConfigFactory.load().getTimeZone());
		Date d = sdf.parse(dateStr);
		System.out.println(d.getTime());
        curator.close();
	}
	
	@Test
	public void testZKManagerDelete() throws Exception{
		System.setProperty("config.resource","/hive-jobrunning.conf");
		Config config = ConfigFactory.load();
		ZKStateConfig zkStateConfig = new ZKStateConfig();
		zkStateConfig.zkQuorum = config.getString("dataSourceConfig.zkQuorum");
		zkStateConfig.zkRoot = config.getString("dataSourceConfig.zkRoot");
		zkStateConfig.zkSessionTimeoutMs = config.getInt("dataSourceConfig.zkSessionTimeoutMs");
		zkStateConfig.zkRetryTimes = config.getInt("dataSourceConfig.zkRetryTimes");
		zkStateConfig.zkRetryInterval = config.getInt("dataSourceConfig.zkRetryInterval");

		RunningJobCrawlConfig crawlConfig = new RunningJobCrawlConfig(null, null, zkStateConfig);
		JobRunningZKStateManager manager = new JobRunningZKStateManager(crawlConfig);

		manager.truncateJobBefore(ResourceType.JOB_CONFIGURATION, "20150907");		
	}

	@Test
	public void testZKLock() throws Exception {
		String date = DateTimeUtil.format(System.currentTimeMillis(), "yyyyMMdd");
		System.out.println(date);
		Thread t = new Thread() {
			@Override
			public void run() {
				CuratorFramework curator2 =  CuratorFrameworkFactory.newClient(
		            	"localhost:2181",
		                15000,
		                15000,
		                new RetryNTimes(3, 1000)
		            );
				curator2.start();
				String path = "/jobrunning/JOB_CONFIGURATION/jobs";
				InterProcessMutex lock = new InterProcessMutex(curator2, path);
				while(true) {
					try {
						lock.acquire();
			            if (curator2.checkExists().forPath(path) != null) {
			            	curator2.getChildren().forPath(path);
			            }
						byte[] bytes = curator2.getData().forPath("/jobrunning/JOB_CONFIGURATION/jobs/job_1437549935027_553208");
						String date = new String(bytes, StandardCharsets.UTF_8);
						System.out.println(date);
					} catch (Exception e) {
						
					}
					finally {
			        	try{
			        		lock.release();
			        	}catch(Exception e){
			        		throw new RuntimeException(e);
			        	}
					}
					try {
						Thread.sleep(20);
					}
					catch (Exception ex) {
						
					}
				}
			}
		};
		t.start();
		CuratorFramework curator =  CuratorFrameworkFactory.newClient(
            	"localhost:2181",
                15000,
                15000,
                new RetryNTimes(3, 1000)
            );
		curator.start();
		while(true) {
			String path = "/jobrunning/JOB_CONFIGURATION/jobs/job_1437549935027_553208";
			InterProcessMutex lock = new InterProcessMutex(curator, path);
			try {
				lock.acquire();
				if (curator.checkExists().forPath(path) == null) {
					curator.create().creatingParentsIfNeeded().forPath(path, date.getBytes(StandardCharsets.UTF_8));
				}
				lock.acquire();
				//Thread.sleep(20);
				curator.delete().deletingChildrenIfNeeded().forPath(path);
			}
			finally {
	        	try{
	        		lock.release();
	        	}catch(Exception e){
	        		throw new RuntimeException(e);
	        	}
			}
		}
	}
}
