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
package org.apache.eagle.jobrunning.zkres;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.eagle.jobrunning.common.JobConstants;
import org.apache.eagle.jobrunning.config.RunningJobCrawlConfig;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.common.DateTimeUtil;

public class JobRunningZKStateManager implements JobRunningZKStateLCM{
	public static final Logger LOG = LoggerFactory.getLogger(JobRunningZKStateManager.class);
	private String zkRoot;
	private CuratorFramework _curator;
	
	public static final String DATE_FORMAT_PATTERN = "yyyyMMdd";
	
	private CuratorFramework newCurator(RunningJobCrawlConfig config) throws Exception {
        return CuratorFrameworkFactory.newClient(
        	config.zkStateConfig.zkQuorum,
            config.zkStateConfig.zkSessionTimeoutMs,
            15000,
            new RetryNTimes(config.zkStateConfig.zkRetryTimes, config.zkStateConfig.zkRetryInterval)
        );
    }
	  
	public JobRunningZKStateManager(RunningJobCrawlConfig config) {
		this.zkRoot = config.zkStateConfig.zkRoot;
        try {
            _curator = newCurator(config);
            _curator.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
	
	public void close() {
        _curator.close();
        _curator = null;
    }
	
	public long getTimestampFromDate(String dateStr) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_PATTERN);
        sdf.setTimeZone(EagleConfigFactory.load().getTimeZone());
		Date d = sdf.parse(dateStr);
		return d.getTime();
	}
	
	@Override
	public List<String> readProcessedJobs(JobConstants.ResourceType type) {
		String path = zkRoot + "/" + type.name() + "/jobs";
		InterProcessMutex lock = new InterProcessMutex(_curator, path);
		try {
			lock.acquire();
            if (_curator.checkExists().forPath(path) != null) {
            	LOG.info("Got processed job list from zk, type: " + type.name());
            	return _curator.getChildren().forPath(path);
            } else {
            	LOG.info("Currently processed job list is empty, type: " + type.name());
                return new ArrayList<String>();
            }
        } catch (Exception e) {
        	LOG.error("fail read processed jobs", e);
            throw new RuntimeException(e);
        }
		finally {
        	try{
        		lock.release();
        	}catch(Exception e){
        		LOG.error("fail releasing lock", e);
        		throw new RuntimeException(e);
        	}
		}
	}

	@Override
	public void addProcessedJob(JobConstants.ResourceType type, String jobID) {
		String path = zkRoot + "/" + type.name() + "/jobs/" + jobID;
		try {
			//we record date for cleanup, e.g. cleanup job's znodes whose created date < 20150801
			String date = DateTimeUtil.format(System.currentTimeMillis(), DATE_FORMAT_PATTERN);
			LOG.info("add processed job, jobID: " + jobID + ", type: " + type + ", date: " + date);
            if (_curator.checkExists().forPath(path) == null) {
                _curator.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path, date.getBytes(StandardCharsets.UTF_8));
            }
            else {
                LOG.warn("Job already exist in zk, skip the job: " + jobID + " , type: " + type);
            }
        } catch (Exception e) {
        	LOG.error("fail adding processed jobs", e);
            throw new RuntimeException(e);
        }
	}

	@Override
	public void truncateJobBefore(JobConstants.ResourceType type, String date) {
		String path = zkRoot + "/" + type.name() + "/jobs";
		InterProcessMutex lock = new InterProcessMutex(_curator, path);
		try {
			lock.acquire();
    		long thresholdTime = getTimestampFromDate(date);
            if (_curator.checkExists().forPath(path) != null) {
    			LOG.info("Going to delete processed job before " + date + ", type: " + type);
            	List<String> jobIDList = _curator.getChildren().forPath(path);
            	for(String jobID : jobIDList) {
            		if (!jobID.startsWith("job_")) continue; // skip lock node
            		String jobPath = path + "/" + jobID;
            		long createTime = getTimestampFromDate(new String(_curator.getData().forPath(jobPath), StandardCharsets.UTF_8));
            		if (createTime < thresholdTime) {
            			LOG.info("Going to truncate job: " + jobPath);
        				_curator.delete().deletingChildrenIfNeeded().forPath(jobPath);
            		}
            	}
            }
            else {
            	LOG.info("Currently processed job list is empty, type: " + type.name());                
            }
        } catch (Exception e) {
        	LOG.error("fail deleting processed jobs", e);
            throw new RuntimeException(e);
        }
		finally {
        	try{
        		lock.release();
        	}catch(Exception e){
        		LOG.error("fail releasing lock", e);
        		throw new RuntimeException(e);
        	}
		}
	}
	
	@Override
	public void truncateProcessedJob(JobConstants.ResourceType type, String jobID) {
		LOG.info("trying to truncate all data for job " + jobID);
		String path = zkRoot + "/" + type.name() + "/jobs/" + jobID;
		InterProcessMutex lock = new InterProcessMutex(_curator, path);
		try {
			lock.acquire();
            if (_curator.checkExists().forPath(path) != null) {
                _curator.delete().deletingChildrenIfNeeded().forPath(path);
                LOG.info("really truncated all data for jobID: " + jobID);
            }
        } catch (Exception e) {
        	LOG.error("fail truncating processed jobs", e);
    		throw new RuntimeException(e);
        }
		finally {
        	try{
        		lock.release();
        	}catch(Exception e){
        		LOG.error("fail releasing lock", e);
        		throw new RuntimeException(e);
        	}
		}
	}

	@Override
	public void truncateEverything() {
		String path = zkRoot;
		InterProcessMutex lock = new InterProcessMutex(_curator, path);
		try{
			lock.acquire();
			if(_curator.checkExists().forPath(path) != null){
				_curator.delete().deletingChildrenIfNeeded().forPath(path);
			}
		}catch(Exception ex){
			LOG.error("fail truncating verything", ex);
			throw new RuntimeException(ex);
		}
		finally {
        	try{
        		lock.release();
        	}catch(Exception e){
        		LOG.error("fail releasing lock", e);
        		throw new RuntimeException(e);
        	}
		}
	}
}
