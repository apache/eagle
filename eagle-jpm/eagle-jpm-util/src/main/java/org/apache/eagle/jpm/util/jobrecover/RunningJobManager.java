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

package org.apache.eagle.jpm.util.jobrecover;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.eagle.jpm.util.resourcefetch.model.AppInfo;
import org.apache.zookeeper.CreateMode;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RunningJobManager implements Serializable {
    public static final Logger LOG = LoggerFactory.getLogger(RunningJobManager.class);
    private String zkRoot;
    private CuratorFramework curator;
    private static final String ENTITY_TAGS_KEY = "entityTags";
    private static final String APP_INFO_KEY = "appInfo";
    private static final String ZNODE_LAST_FINISH_TIME = "lastFinishTime";
    private final InterProcessMutex lock;

    private CuratorFramework newCurator(String zkQuorum, int zkSessionTimeoutMs, int zkRetryTimes, int zkRetryInterval) {
        return CuratorFrameworkFactory.newClient(
            zkQuorum,
            zkSessionTimeoutMs,
            15000,
            new RetryNTimes(zkRetryTimes, zkRetryInterval)
        );
    }

    public RunningJobManager(String zkQuorum, int zkSessionTimeoutMs, int zkRetryTimes, int zkRetryInterval, String zkRoot, String lockPath) {
        this.zkRoot = zkRoot;
        curator = newCurator(zkQuorum, zkSessionTimeoutMs, zkRetryTimes, zkRetryInterval);
        try {
            curator.start();
        } catch (Exception e) {
            LOG.error("curator start error {}", e);
        }
        LOG.info("InterProcessMutex lock path is " + lockPath);
        lock = new InterProcessMutex(curator, lockPath);
        try {
            if (curator.checkExists().forPath(this.zkRoot) == null) {
                curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(this.zkRoot);
            }
        } catch (Exception e) {
            LOG.warn("{}", e);
        }
    }

    public Map<String, Pair<Map<String, String>, AppInfo>> recoverYarnApp(String yarnAppId) throws Exception {
        Map<String, Pair<Map<String, String>, AppInfo>> result = new HashMap<>();
        String path = this.zkRoot + "/" + yarnAppId;
        try {
            lock.acquire();
            if (curator.checkExists().forPath(path) == null) {
                return result;
            }
            List<String> jobIds = curator.getChildren().forPath(path);
            if (jobIds.size() == 0) {
                LOG.info("delete empty path {}", path);
                delete(yarnAppId);
            }

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
                appInfo.setAmContainerLogs(appInfoMap.get("amContainerLogs") == null ? "" : appInfoMap.get("amContainerLogs"));
                appInfo.setAmHostHttpAddress(appInfoMap.get("amHostHttpAddress") == null ? "" : appInfoMap.get("amHostHttpAddress"));
                appInfo.setAllocatedMB(Long.parseLong(appInfoMap.get("allocatedMB")));
                appInfo.setAllocatedVCores(Integer.parseInt(appInfoMap.get("allocatedVCores")));
                appInfo.setRunningContainers(Integer.parseInt(appInfoMap.get("runningContainers")));

                Map<String, String> tags = parseResult.get(ENTITY_TAGS_KEY);
                result.put(jobId, Pair.of(tags, appInfo));
            }
        } catch (Exception e) {
            LOG.error("fail to recoverYarnApp", e);
            throw new RuntimeException(e);
        } finally {
            try {
                lock.release();
            } catch (Exception e) {
                LOG.error("fail releasing lock", e);
            }
        }

        return result;
    }

    public Map<String, Map<String, Pair<Map<String, String>, AppInfo>>> recover() {
        //we need read from zookeeper, path looks like /apps/x/running/yarnAppId/jobId/
        //content of path /apps/x/running/yarnAppId/jobId is Pair<Map<String, String>, AppInfo>
        //Pair is entity tags and AppInfo
        //as we know, a yarn application may contains many mr jobs or spark applications
        //so, the returned results is a Map-Map
        //<yarnAppId, <jobId, Pair<<Map<String, String>, AppInfo>>>
        Map<String, Map<String, Pair<Map<String, String>, AppInfo>>> result = new HashMap<>();
        try {
            lock.acquire();
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
        } finally {
            try {
                lock.release();
            } catch (Exception e) {
                LOG.error("fail releasing lock", e);
            }
        }
        return result;
    }

    public boolean update(String yarnAppId, String jobId, Map<String, String> tags, AppInfo app) {
        String path = this.zkRoot + "/" + yarnAppId + "/" + jobId;
        Map<String, String> appInfo = new HashMap<>();
        appInfo.put("id", app.getId());
        appInfo.put("user", app.getUser());
        appInfo.put("name", app.getName());
        appInfo.put("queue", app.getQueue());
        appInfo.put("state", app.getState());
        appInfo.put("finalStatus", app.getFinalStatus());
        appInfo.put("progress", app.getProgress() + "");
        appInfo.put("trackingUI", app.getTrackingUI());
        appInfo.put("diagnostics", app.getDiagnostics());
        appInfo.put("trackingUrl", app.getTrackingUrl());
        appInfo.put("clusterId", app.getClusterId());
        appInfo.put("applicationType", app.getApplicationType());
        appInfo.put("startedTime", app.getStartedTime() + "");
        appInfo.put("finishedTime", app.getFinishedTime() + "");
        appInfo.put("elapsedTime", app.getElapsedTime() + "");
        appInfo.put("amContainerLogs", app.getAmContainerLogs() == null ? "" : app.getAmContainerLogs());
        appInfo.put("amHostHttpAddress", app.getAmHostHttpAddress() == null ? "" : app.getAmHostHttpAddress());
        appInfo.put("allocatedMB", app.getAllocatedMB() + "");
        appInfo.put("allocatedVCores", app.getAllocatedVCores() + "");
        appInfo.put("runningContainers", app.getRunningContainers() + "");

        Map<String, String> fields = new HashMap<>();
        fields.put(ENTITY_TAGS_KEY, (new JSONObject(tags)).toString());
        fields.put(APP_INFO_KEY, (new JSONObject(appInfo)).toString());
        try {
            lock.acquire();
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
                lock.release();
            } catch (Exception e) {
                LOG.error("fail releasing lock", e);
            }
        }
        return true;
    }

    public void delete(String yarnAppId, String jobId) {
        String path = this.zkRoot + "/" + yarnAppId + "/" + jobId;
        try {
            lock.acquire();
            if (curator.checkExists().forPath(path) != null) {
                curator.delete().deletingChildrenIfNeeded().forPath(path);
                LOG.info("delete job {} for yarn app {}, path {} ", jobId, yarnAppId, path);
                String yarnPath = this.zkRoot + "/" + yarnAppId;
                if (curator.getChildren().forPath(yarnPath).size() == 0) {
                    delete(yarnAppId);
                }
            }
        } catch (Exception e) {
            LOG.error("failed to delete job {} for yarn app {}, path {}, {}", jobId, yarnAppId, path, e);
        } finally {
            try {
                lock.release();
            } catch (Exception e) {
                LOG.error("fail releasing lock", e);

            }
        }
    }

    public void delete(String yarnAppId) {
        String path = this.zkRoot + "/" + yarnAppId;
        try {
            lock.acquire();
            if (curator.checkExists().forPath(path) != null) {
                curator.delete().forPath(path);
                LOG.info("delete yarn app {}, path {} ", yarnAppId, path);
            }
        } catch (Exception e) {
            LOG.error("failed to delete yarn app {}, path {} ", yarnAppId, path);
        } finally {
            try {
                lock.release();
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
            String value = (String) object.get(key);

            JSONObject jsonObject = new JSONObject(value);
            Map<String, String> items = result.get(key);
            Iterator<String> keyItemItr = jsonObject.keys();
            while (keyItemItr.hasNext()) {
                String itemKey = keyItemItr.next();
                items.put(itemKey, (String) jsonObject.get(itemKey));
            }
        }
        return result;
    }

    public Long recoverLastFinishedTime(int partitionId) {
        String path = this.zkRoot + "/" + partitionId + "/" + ZNODE_LAST_FINISH_TIME;
        try {
            return Long.valueOf(new String(curator.getData().forPath(path)));
        } catch (Exception e) {
            LOG.error("failed to recover last finish time {}", e);
        }

        return 0L;
    }

    public void updateLastFinishTime(int partitionId, Long lastFinishTime) {
        String path = this.zkRoot + "/" + partitionId + "/" + ZNODE_LAST_FINISH_TIME;
        try {
            if (curator.checkExists().forPath(path) == null) {
                curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path);
            }
            curator.setData().forPath(path, lastFinishTime.toString().getBytes("UTF-8"));
        } catch (Exception e) {
            LOG.error("failed to update last finish time {}", e);
        }
    }

    public void close() {
        if (curator != null) {
            curator.close();
        }
    }
}
