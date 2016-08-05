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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.eagle.jpm.mr.running.config.MRRunningConfigManager;
import org.apache.eagle.jpm.mr.running.entities.JobExecutionAPIEntity;
import org.apache.eagle.jpm.util.resourceFetch.model.AppInfo;
import org.apache.zookeeper.CreateMode;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class RunningJobManager implements Serializable {
    public static final Logger LOG = LoggerFactory.getLogger(RunningJobManager.class);
    private String zkRoot;
    private CuratorFramework curator;
    private final static String ENTITY_TAGS_KEY = "entityTags";
    private final static String APP_INFO_KEY = "appInfo";

    private CuratorFramework newCurator(MRRunningConfigManager.ZKStateConfig config) throws Exception {
        return CuratorFrameworkFactory.newClient(
                config.zkQuorum,
                config.zkSessionTimeoutMs,
                15000,
                new RetryNTimes(config.zkRetryTimes, config.zkRetryInterval)
        );
    }

    public RunningJobManager(MRRunningConfigManager.ZKStateConfig config) {
        this.zkRoot = config.zkRoot;

        try {
            curator = newCurator(config);
            curator.start();
            if (curator.checkExists().forPath(this.zkRoot) == null) {
                curator.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(this.zkRoot);
            }
        } catch (Exception e) {
        }
    }

    public Map<String, JobExecutionAPIEntity> recoverYarnApp(String yarnAppId) throws Exception {
        Map<String, JobExecutionAPIEntity> result = new HashMap<>();
        String path = this.zkRoot + "/" + yarnAppId;
        List<String> jobIds = curator.getChildren().forPath(path);
        /*if (jobIds.size() == 0) {
            LOG.info("delete empty path {}", path);
            delete(yarnAppId);
        }*/

        for (String jobId : jobIds) {
            String jobPath = path + "/" + jobId;
            LOG.info("recover path {}", jobPath);
            String fields = new String(curator.getData().forPath(jobPath), "UTF-8");
            if (fields.length() == 0) {
                //LOG.info("delete empty path {}", jobPath);
                //delete(yarnAppId, jobId);
                continue;
            }
            JSONObject object = new JSONObject(fields);
            Map<String, Map<String, String>> parseResult = parse(object);
            Map<String, String> tags = parseResult.get(ENTITY_TAGS_KEY);
            JobExecutionAPIEntity jobExecutionAPIEntity = new JobExecutionAPIEntity();
            jobExecutionAPIEntity.setTags(tags);

            Map<String, String> appInfoMap = parseResult.get(APP_INFO_KEY);
            AppInfo appInfo = new AppInfo();
            appInfo.setId(appInfoMap.get("id"));
            appInfo.setUser(appInfoMap.get("user"));
            appInfo.setName(appInfoMap.get("name"));
            appInfo.setQueue(appInfoMap.get("queue"));
            appInfo.setState(appInfoMap.get("state"));
            appInfo.setFinalStatus(appInfoMap.get("finalStatus"));
            appInfo.setProgress(Double.parseDouble(appInfoMap.get("progress")));
            appInfo.setTrackingUI(appInfoMap.get("trackingUI"));
            appInfo.setDiagnostics(appInfoMap.get("diagnostics"));
            appInfo.setTrackingUrl(appInfoMap.get("trackingUrl"));
            appInfo.setClusterId(appInfoMap.get("clusterId"));
            appInfo.setApplicationType(appInfoMap.get("applicationType"));
            appInfo.setStartedTime(Long.parseLong(appInfoMap.get("startedTime")));
            appInfo.setFinishedTime(Long.parseLong(appInfoMap.get("finishedTime")));
            appInfo.setElapsedTime(Long.parseLong(appInfoMap.get("elapsedTime")));
            appInfo.setAmContainerLogs(appInfoMap.get("amContainerLogs"));
            appInfo.setAmHostHttpAddress(appInfoMap.get("amHostHttpAddress"));
            appInfo.setAllocatedMB(Long.parseLong(appInfoMap.get("allocatedMB")));
            appInfo.setAllocatedVCores(Integer.parseInt(appInfoMap.get("allocatedVCores")));
            appInfo.setRunningContainers(Integer.parseInt(appInfoMap.get("runningContainers")));

            jobExecutionAPIEntity.setAppInfo(appInfo);
            jobExecutionAPIEntity.setTimestamp(appInfo.getStartedTime());
            result.put(jobId, jobExecutionAPIEntity);
        }
        return result;
    }

    public Map<String, Map<String, JobExecutionAPIEntity>> recover() {
        //we need read from zookeeper, path looks like /apps/mr/running/yarnAppId/jobId/
        //content of path /apps/mr/running/yarnAppId/jobId is JobExecutionAPIEntity
        //as we know, a yarn application may contains many mr jobs
        //so, the returned results is a Map-Map
        //<yarnAppId, <jobId, JobExecutionAPIEntity>>
        Map<String, Map<String, JobExecutionAPIEntity>> result = new HashMap<>();
        try {
            List<String> yarnAppIds = curator.getChildren().forPath(this.zkRoot);
            for (String yarnAppId : yarnAppIds) {
                if (!result.containsKey(yarnAppId)) {
                    result.put(yarnAppId, new HashMap<>());
                }

                result.put(yarnAppId, recoverYarnApp(yarnAppId));
            }
        } catch (Exception e) {
            LOG.error("fail to recover", e);
            throw new RuntimeException(e);
        }
        return result;
    }

    public boolean update(String yarnAppId, String jobId, JobExecutionAPIEntity entity) {
        String path = this.zkRoot + "/" + yarnAppId + "/" + jobId;
        //InterProcessMutex lock = new InterProcessMutex(curator, path);
        Map<String, String> fields = new HashMap<>();
        Map<String, String> appInfo = new HashMap<>();
        appInfo.put("id", entity.getAppInfo().getId());
        appInfo.put("user", entity.getAppInfo().getUser());
        appInfo.put("name", entity.getAppInfo().getName());
        appInfo.put("queue", entity.getAppInfo().getQueue());
        appInfo.put("state", entity.getAppInfo().getState());
        appInfo.put("finalStatus", entity.getAppInfo().getFinalStatus());
        appInfo.put("progress", entity.getAppInfo().getProgress() + "");
        appInfo.put("trackingUI", entity.getAppInfo().getTrackingUI());
        appInfo.put("diagnostics", entity.getAppInfo().getDiagnostics());
        appInfo.put("trackingUrl", entity.getAppInfo().getTrackingUrl());
        appInfo.put("clusterId", entity.getAppInfo().getClusterId());
        appInfo.put("applicationType", entity.getAppInfo().getApplicationType());
        appInfo.put("startedTime", entity.getAppInfo().getStartedTime() + "");
        appInfo.put("finishedTime", entity.getAppInfo().getFinishedTime() + "");
        appInfo.put("elapsedTime", entity.getAppInfo().getElapsedTime() + "");
        appInfo.put("amContainerLogs", entity.getAppInfo().getAmContainerLogs());
        appInfo.put("amHostHttpAddress", entity.getAppInfo().getAmHostHttpAddress());
        appInfo.put("allocatedMB", entity.getAppInfo().getAllocatedMB() + "");
        appInfo.put("allocatedVCores", entity.getAppInfo().getAllocatedVCores() + "");
        appInfo.put("runningContainers", entity.getAppInfo().getRunningContainers() + "");

        fields.put(ENTITY_TAGS_KEY, (new JSONObject(entity.getTags())).toString());
        fields.put(APP_INFO_KEY, (new JSONObject(appInfo)).toString());
        try {
            //lock.acquire();
            JSONObject object = new JSONObject(fields);
            if (curator.checkExists().forPath(path) == null) {
                curator.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path);
            }
            curator.setData().forPath(path, object.toString().getBytes("UTF-8"));

        } catch (Exception e) {
            LOG.error("failed to update job {} for yarn app {} ", jobId, yarnAppId);
        } finally {
            try {
                //lock.release();
            } catch (Exception e) {
                LOG.error("fail releasing lock", e);
            }
        }
        return true;
    }

    public void delete(String yarnAppId, String jobId) {
        String path = this.zkRoot + "/" + yarnAppId + "/" + jobId;
        //InterProcessMutex lock = new InterProcessMutex(curator, path);
        try {
            //lock.acquire();
            if (curator.checkExists().forPath(path) != null) {
                curator.delete().deletingChildrenIfNeeded().forPath(path);
                LOG.info("delete job {} for yarn app {}, path {} ", jobId, yarnAppId, path);
                if (curator.getChildren().forPath(path).size() == 0) {
                    delete(yarnAppId);
                }
            }
        } catch (Exception e) {
            LOG.error("failed to delete job {} for yarn app {}, path {}, {}", jobId, yarnAppId, path, e);
        } finally {
            try {
                //lock.release();
            } catch (Exception e) {
                LOG.error("fail releasing lock", e);

            }
        }
    }

    public void delete(String yarnAppId) {
        String path = this.zkRoot + "/" + yarnAppId;
        //InterProcessMutex lock = new InterProcessMutex(curator, path);
        try {
            //lock.acquire();
            if (curator.checkExists().forPath(path) != null) {
                curator.delete().forPath(path);
                LOG.info("delete yarn app {}, path {} ", yarnAppId, path);
            }
        } catch (Exception e) {
            LOG.error("failed to delete yarn app {}, path {} ", yarnAppId, path);
        } finally {
            try {
                //lock.release();
            } catch (Exception e) {
                LOG.error("fail releasing lock", e);
            }
        }
    }

    public Map<String, Map<String, String>> parse(JSONObject object) throws JSONException {
        Map<String, Map<String, String>> result = new HashMap<>();

        Iterator<String> keysItr = object.keys();
        while (keysItr.hasNext()) {
            String key = keysItr.next();
            result.put(key, new HashMap<>());
            String value = (String)object.get(key);

            JSONObject jsonObject = new JSONObject(value);
            Map<String, String> items = result.get(key);
            Iterator<String> keyItemItr = jsonObject.keys();
            while (keyItemItr.hasNext()) {
                String itemKey = keyItemItr.next();
                items.put(itemKey, (String)jsonObject.get(itemKey));
            }
        }
        return result;
    }
}
