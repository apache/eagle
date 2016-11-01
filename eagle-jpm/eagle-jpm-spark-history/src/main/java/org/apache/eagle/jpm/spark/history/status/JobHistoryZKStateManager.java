/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.jpm.spark.history.status;

import org.apache.eagle.jpm.spark.history.SparkHistoryJobAppConfig;
import org.apache.eagle.jpm.spark.history.crawl.SparkApplicationInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JobHistoryZKStateManager {
    private static final Logger LOG = LoggerFactory.getLogger(JobHistoryZKStateManager.class);

    private static final String START_TIMESTAMP = "lastAppTime";
    private String zkRoot;
    private CuratorFramework curator;

    private CuratorFramework newCurator(SparkHistoryJobAppConfig config) throws Exception {
        return CuratorFrameworkFactory.newClient(
                config.zkStateConfig.zkQuorum,
                config.zkStateConfig.zkSessionTimeoutMs,
                15000,
                new RetryNTimes(config.zkStateConfig.zkRetryTimes, config.zkStateConfig.zkRetryInterval)
        );
    }

    public JobHistoryZKStateManager(SparkHistoryJobAppConfig config) {
        this.zkRoot = config.zkStateConfig.zkRoot + "/" + config.stormConfig.siteId;

        try {
            curator = newCurator(config);
            curator.start();
        } catch (Exception e) {
            LOG.error("Fail to connect to zookeeper", e);
            throw new RuntimeException(e);
        }
    }

    public void close() {
        curator.close();
        curator = null;
    }

    public List<String> loadApplications(int limit) {
        String jobPath = zkRoot + "/jobs";
        List<String> apps = new ArrayList<>();
        //InterProcessLock lock = new InterProcessReadWriteLock(curator,jobPath).writeLock();
        try {
            //lock.acquire();
            Iterator<String> iter = curator.getChildren().forPath(jobPath).iterator();
            while (iter.hasNext()) {
                String appId = iter.next();
                String path = jobPath + "/" + appId;
                if (curator.checkExists().forPath(path) != null) {
                    if (new String(curator.getData().forPath(path)).equals(ZKStateConstant.AppStatus.INIT.toString())) {
                        apps.add(appId);
                    }
                }
                if (apps.size() == limit) {
                    break;
                }
            }
            return apps;
        } catch (Exception e) {
            LOG.error("fail to read unprocessed jobs", e);
            throw new RuntimeException(e);
        } finally {
           /* try {
                lock.release();
            } catch (Exception e) {
                LOG.error("fail to release lock", e);
            }*/

        }
    }

    public void resetApplications() {
        String jobPath = zkRoot + "/jobs";
        InterProcessLock lock = new InterProcessReadWriteLock(curator,jobPath).writeLock();
        try {
            lock.acquire();
            (curator.getChildren().forPath(jobPath)).forEach(appId -> {
                String path = jobPath + "/" + appId;
                try {
                    if (curator.checkExists().forPath(path) != null) {
                        String status = new String(curator.getData().forPath(path));
                        if (!ZKStateConstant.AppStatus.INIT.toString().equals(status)) {
                            curator.setData().forPath(path, ZKStateConstant.AppStatus.INIT.toString().getBytes("UTF-8"));
                        }
                    }
                } catch (Exception e) {
                    LOG.error("fail to read unprocessed job", e);
                    throw new RuntimeException(e);
                }
            });

        } catch (Exception e) {
            LOG.error("fail to read unprocessed jobs", e);
            throw new RuntimeException(e);
        } finally {
            try {
                lock.release();
            } catch (Exception e) {
                LOG.error("fail to release lock", e);
            }
        }
    }

    public SparkApplicationInfo getApplicationInfo(String appId) {

        String appPath = zkRoot + "/jobs/" + appId + "/info";
        try {
            SparkApplicationInfo info = new SparkApplicationInfo();
            if (curator.checkExists().forPath(appPath) != null) {
                String[] appStatus = new String(curator.getData().forPath(appPath)).split("/");
                info.setQueue(appStatus[0]);
                info.setState(appStatus[1]);
                info.setFinalStatus(appStatus[2]);
                if (appStatus.length > 3) {
                    info.setUser(appStatus[3]);
                    info.setName(appStatus[4]);
                }

            }
            return info;
        } catch (Exception e) {
            LOG.error("fail to read application attempt info", e);
            throw new RuntimeException(e);
        }
    }

    public long readLastFinishedTimestamp() {
        String lastTimeStampPath = zkRoot + "/" + START_TIMESTAMP;

        try {
            if (curator.checkExists().forPath(lastTimeStampPath) == null) {
                return 0L;
            } else {
                return Long.valueOf(new String(curator.getData().forPath(lastTimeStampPath)));
            }
        } catch (Exception e) {
            LOG.error("fail to read last finished spark job timestamp", e);
            throw new RuntimeException(e);
        }
    }

    public boolean hasApplication(String appId) {
        String path = zkRoot + "/jobs/" + appId;
        try {
            return curator.checkExists().forPath(path) != null;
        } catch (Exception e) {
            LOG.error("fail to check whether application exists", e);
            throw new RuntimeException(e);
        }
    }

    public void addFinishedApplication(String appId, String queue, String yarnState, String yarnStatus, String user, String name) {
        String path = zkRoot + "/jobs/" + appId;
        try {
            if (curator.checkExists().forPath(path) != null) {
                curator.delete().deletingChildrenIfNeeded().forPath(path);
            }

            name = name.replace("/", "_");
            if (name.length() > 50) {
                name = name.substring(0, 50);
            }

            CuratorTransactionBridge result =  curator.inTransaction().create().withMode(CreateMode.PERSISTENT).forPath(path, ZKStateConstant.AppStatus.INIT.toString().getBytes("UTF-8"));
            result = result.and().create().withMode(CreateMode.PERSISTENT).forPath(path + "/info", String.format("%s/%s/%s/%s/%s", queue, yarnState, yarnStatus, user, name).getBytes("UTF-8"));

            result.and().commit();
        } catch (Exception e) {
            LOG.error("fail adding finished application", e);
            throw new RuntimeException(e);
        }
    }


    public void updateLastUpdateTime(Long updateTime) {
        String lastTimeStampPath = zkRoot + "/" + START_TIMESTAMP;
        try {
            if (curator.checkExists().forPath(lastTimeStampPath) == null) {
                curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(lastTimeStampPath, updateTime.toString().getBytes("UTF-8"));
            } else {
                long originalEndTime = this.readLastFinishedTimestamp();
                if (originalEndTime < updateTime) {
                    curator.setData().forPath(lastTimeStampPath, updateTime.toString().getBytes("UTF-8"));
                }
            }
        } catch (Exception e) {
            LOG.error("fail to update last finished time", e);
            throw new RuntimeException(e);
        }

    }

    public void updateApplicationStatus(String appId, Enum<ZKStateConstant.AppStatus> status) {
        String path = zkRoot + "/jobs/" + appId ;
        InterProcessLock lock = new InterProcessReadWriteLock(curator,zkRoot + "/jobs").readLock();
        try {
            if (curator.checkExists().forPath(path) != null) {
                if (status.equals(ZKStateConstant.AppStatus.FINISHED)) {
                    lock.acquire();
                    curator.delete().deletingChildrenIfNeeded().forPath(path);
                } else {
                    curator.setData().forPath(path, status.toString().getBytes("UTF-8"));
                }
            } else {
                LOG.warn("failed to update with status {} due to path {} not existing ", status, path);
                //throw new RuntimeException("Failed to update for application with path: " + path);
            }
        } catch (Exception e) {
            LOG.error("fail to update application status as {}", status, e);
            throw new RuntimeException(e);
        } finally {
            try {
                if (lock.isAcquiredInThisProcess()) {
                    lock.release();
                }
            } catch (Exception e) {
                LOG.error("fail to release lock",e);
            }
        }
    }
}
