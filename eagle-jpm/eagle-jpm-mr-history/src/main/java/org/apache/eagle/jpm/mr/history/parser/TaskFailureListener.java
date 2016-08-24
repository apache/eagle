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

package org.apache.eagle.jpm.mr.history.parser;

import org.apache.eagle.jpm.mr.history.common.JHFConfigManager;
import org.apache.eagle.jpm.mr.historyentity.JobBaseAPIEntity;
import org.apache.eagle.jpm.mr.historyentity.TaskAttemptExecutionAPIEntity;
import org.apache.eagle.jpm.mr.historyentity.TaskFailureCountAPIEntity;
import org.apache.eagle.jpm.util.MRJobTagName;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TaskFailureListener implements HistoryJobEntityCreationListener {
    private static final Logger logger = LoggerFactory.getLogger(TaskFailureListener.class);
    private static final String MR_ERROR_CATEGORY_CONFIG_FILE_NAME = "MRErrorCategory.config";
    private static final int BATCH_SIZE = 1000;
    private static final int MAX_RETRY_TIMES = 3;

    private final List<TaskFailureCountAPIEntity> failureTasks = new ArrayList<TaskFailureCountAPIEntity>();
    private final MRErrorClassifier classifier;
	private JHFConfigManager configManager;

    public TaskFailureListener(JHFConfigManager configManager) {
        this.configManager = configManager;
    	InputStream is = null;
    	try {
    		is = TaskFailureListener.class.getClassLoader().getResourceAsStream(MR_ERROR_CATEGORY_CONFIG_FILE_NAME);
            URL url = TaskFailureListener.class.getClassLoader().getResource(MR_ERROR_CATEGORY_CONFIG_FILE_NAME);
            if (url != null) {
                logger.info("Feeder is going to load configuration file: " + url.toString());
            }
    		classifier = new MRErrorClassifier(is);
    	} catch (IOException ex) {
    		throw new RuntimeException("Can't find MRErrorCategory.config file to configure MRErrorCategory");
    	} finally {
    		if (is != null) {
    			try {
    				is.close();
    			} catch (IOException e) {
    			}
    		}
    	}
    }

    @Override
    public void jobEntityCreated(JobBaseAPIEntity entity) throws Exception {
    	if (!(entity instanceof TaskAttemptExecutionAPIEntity))
    		return;

    	TaskAttemptExecutionAPIEntity e = (TaskAttemptExecutionAPIEntity)entity;
    	// only store those killed or failed tasks
    	if (!e.getTaskStatus().equals(EagleTaskStatus.FAILED.name()) && !e.getTaskStatus().equals(EagleTaskStatus.KILLED.name()))
    		return;

    	TaskFailureCountAPIEntity failureTask = new TaskFailureCountAPIEntity();
    	Map<String, String> tags = new HashMap<>();
    	failureTask.setTags(tags);
    	tags.put(MRJobTagName.SITE.toString(), e.getTags().get(MRJobTagName.SITE.toString()));
    	tags.put(MRJobTagName.JOD_DEF_ID.toString(), e.getTags().get(MRJobTagName.JOD_DEF_ID.toString()));
    	tags.put(MRJobTagName.RACK.toString(), e.getTags().get(MRJobTagName.RACK.toString()));
    	tags.put(MRJobTagName.HOSTNAME.toString(), e.getTags().get(MRJobTagName.HOSTNAME.toString()));
    	tags.put(MRJobTagName.JOB_ID.toString(), e.getTags().get(MRJobTagName.JOB_ID.toString()));
    	tags.put(MRJobTagName.TASK_ATTEMPT_ID.toString(), e.getTaskAttemptID());
    	tags.put(MRJobTagName.TASK_TYPE.toString(), e.getTags().get(MRJobTagName.TASK_TYPE.toString()));

    	//TODO need optimize, match and then capture the data
    	final String errCategory = classifier.classifyError(e.getError());
    	tags.put(MRJobTagName.ERROR_CATEGORY.toString(), errCategory);

    	failureTask.setError(e.getError());
    	failureTask.setFailureCount(1); // hard coded to 1 unless we do pre-aggregation in the future
    	failureTask.setTimestamp(e.getTimestamp());
    	failureTask.setTaskStatus(e.getTaskStatus());
    	failureTasks.add(failureTask);

    	if (failureTasks.size() >= BATCH_SIZE) flush();
    }
    
    @Override
    public void flush() throws Exception {
		JHFConfigManager.EagleServiceConfig eagleServiceConfig = configManager.getEagleServiceConfig();
		JHFConfigManager.JobExtractorConfig jobExtractorConfig = configManager.getJobExtractorConfig();
		IEagleServiceClient client = new EagleServiceClientImpl(
				eagleServiceConfig.eagleServiceHost,
				eagleServiceConfig.eagleServicePort,
				eagleServiceConfig.username,
				eagleServiceConfig.password);

		client.getJerseyClient().setReadTimeout(jobExtractorConfig.readTimeoutSeconds * 1000);

    	int tried = 0;
    	while (tried <= MAX_RETRY_TIMES) {
    		try {
    			logger.info("start flushing entities of total number " + failureTasks.size());
    			client.create(failureTasks);
    			logger.info("finish flushing entities of total number " + failureTasks.size());
    			failureTasks.clear();
				break;
    		} catch (Exception ex) {
    			if (tried < MAX_RETRY_TIMES) {
    				logger.error("Got exception to flush, retry as " + (tried + 1) + " times", ex);
    			} else {
    				logger.error("Got exception to flush, reach max retry times " + MAX_RETRY_TIMES, ex);
    				throw ex;
    			}
    		}
			tried ++;
    	}
        client.getJerseyClient().destroy();
        client.close();
    }
}
