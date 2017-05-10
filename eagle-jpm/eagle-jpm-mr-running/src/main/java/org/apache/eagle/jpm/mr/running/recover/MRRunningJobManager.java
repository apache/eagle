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

package org.apache.eagle.jpm.mr.running.recover;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.eagle.jpm.mr.running.MRRunningJobConfig;
import org.apache.eagle.jpm.mr.runningentity.JobExecutionAPIEntity;
import org.apache.eagle.jpm.util.jobrecover.RunningJobManager;
import org.apache.eagle.jpm.util.resourcefetch.model.AppInfo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MRRunningJobManager implements Serializable {
    private RunningJobManager runningJobManager;

    public MRRunningJobManager(MRRunningJobConfig.ZKStateConfig config) {
        this.runningJobManager = new RunningJobManager(config.zkQuorum,
            config.zkSessionTimeoutMs, config.zkRetryTimes, config.zkRetryInterval, config.zkRoot, config.zkLockPath);
    }

    public Map<String, JobExecutionAPIEntity> recoverYarnApp(String appId) throws Exception {
        Map<String, Pair<Map<String, String>, AppInfo>> result = this.runningJobManager.recoverYarnApp(appId);
        Map<String, JobExecutionAPIEntity> jobs = new HashMap<>();
        for (String jobId : result.keySet()) {
            Pair<Map<String, String>, AppInfo> job = result.get(jobId);
            JobExecutionAPIEntity jobExecutionAPIEntity = new JobExecutionAPIEntity();
            jobExecutionAPIEntity.setTags(job.getLeft());
            jobExecutionAPIEntity.setAppInfo(job.getRight());
            jobExecutionAPIEntity.setTimestamp(job.getRight().getStartedTime());
            jobs.put(jobId, jobExecutionAPIEntity);
        }
        return jobs;
    }

    public Map<String, Map<String, JobExecutionAPIEntity>> recover() {
        //we need read from zookeeper, path looks like /apps/mr/running/yarnAppId/jobId/
        //<yarnAppId, <jobId, JobExecutionAPIEntity>>
        Map<String, Map<String, JobExecutionAPIEntity>> result = new HashMap<>();
        Map<String, Map<String, Pair<Map<String, String>, AppInfo>>> apps = this.runningJobManager.recover();
        for (String appId : apps.keySet()) {
            result.put(appId, new HashMap<>());
            Map<String, Pair<Map<String, String>, AppInfo>> jobs = apps.get(appId);

            for (String jobId : jobs.keySet()) {
                Pair<Map<String, String>, AppInfo> job = jobs.get(jobId);
                JobExecutionAPIEntity jobExecutionAPIEntity = new JobExecutionAPIEntity();
                jobExecutionAPIEntity.setTags(job.getLeft());
                jobExecutionAPIEntity.setAppInfo(job.getRight());
                jobExecutionAPIEntity.setTimestamp(job.getRight().getStartedTime());
                result.get(appId).put(jobId, jobExecutionAPIEntity);
            }
        }
        return result;
    }

    public boolean update(String yarnAppId, String jobId, JobExecutionAPIEntity entity) {
        return this.runningJobManager.update(yarnAppId, jobId, entity.getTags(), entity.getAppInfo());
    }

    public void delete(String yarnAppId, String jobId) {
        this.runningJobManager.delete(yarnAppId, jobId);
    }

    public void close() {
        this.runningJobManager.close();
    }
}
