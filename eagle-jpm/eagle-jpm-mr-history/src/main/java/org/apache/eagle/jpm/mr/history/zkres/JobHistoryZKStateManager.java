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

package org.apache.eagle.jpm.mr.history.zkres;

import org.apache.eagle.jpm.mr.history.MRHistoryJobConfig.ZKStateConfig;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class JobHistoryZKStateManager implements JobHistoryZKStateLCM {
    public static final Logger LOG = LoggerFactory.getLogger(JobHistoryZKStateManager.class);
    private String zkRoot;
    private CuratorFramework curator;
    public static final String ZNODE_LOCK_FOR_ENSURE_JOB_PARTITIONS = "lockForEnsureJobPartitions";
    public static final String ZNODE_FORCE_START_FROM = "forceStartFrom";
    public static final String ZNODE_PARTITIONS = "partitions";
    public static final String ZNODE_JOBS = "jobs";
    public static final String ZNODE_JOB_IDS = "jobIds";
    public static final String ZNODE_TIMESTAMPS = "timeStamps";

    public static final int BACKOFF_DAYS = 0;

    private static JobHistoryZKStateManager jobHistoryZKStateManager = new JobHistoryZKStateManager();

    private CuratorFramework newCurator(ZKStateConfig config) throws Exception {
        return CuratorFrameworkFactory.newClient(
            config.zkQuorum,
            config.zkSessionTimeoutMs,
            15000,
            new RetryNTimes(config.zkRetryTimes, config.zkRetryInterval)
        );
    }

    public static JobHistoryZKStateManager instance() {
        return jobHistoryZKStateManager;
    }

    public void init(ZKStateConfig config) {
        this.zkRoot = config.zkRoot;

        try {
            curator = newCurator(config);
            curator.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        curator.close();
        curator = null;
    }

    private String readForceStartFrom() {
        String path = zkRoot + "/" + ZNODE_FORCE_START_FROM;
        try {
            if (curator.checkExists().forPath(path) != null) {
                return new String(curator.getData().forPath(path), "UTF-8");
            }
        } catch (Exception ex) {
            LOG.error("fail reading forceStartFrom znode", ex);
        }
        return null;
    }

    private void deleteForceStartFrom() {
        String path = zkRoot + "/" + ZNODE_FORCE_START_FROM;
        try {
            if (curator.checkExists().forPath(path) != null) {
                curator.delete().forPath(path);
            }
        } catch (Exception ex) {
            LOG.error("fail reading forceStartFrom znode", ex);
        }
    }

    private String getProcessedDateAfterBackoff(int backOffDays) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DATE, -1);
        c.add(Calendar.DATE, -1 * backOffDays);
        return sdf.format(c.getTime());
    }

    /**
     * under zkRoot, znode forceStartFrom is used to force job is crawled from that date
     * IF
     * forceStartFrom znode is provided, and its value is valid date with format "YYYYMMDD",
     * THEN
     * rebuild all partitions with the forceStartFrom
     * ELSE
     * IF
     * partition structure is changed
     * THEN
     * IF
     * there is valid mindate for existing partitions
     * THEN
     * rebuild job partitions from that valid mindate
     * ELSE
     * rebuild job partitions from (today - BACKOFF_DAYS)
     * END
     * ELSE
     * do nothing
     * END
     * END
     * <p>
     * forceStartFrom is deleted once its value is used, so next time when topology is restarted, program can run from where topology is stopped last time
     * </p>
     * .
     */
    @Override
    public void ensureJobPartitions(int numTotalPartitions) {
        // lock before rebuild job partitions
        String lockForEnsureJobPartitions = zkRoot + "/" + ZNODE_LOCK_FOR_ENSURE_JOB_PARTITIONS;
        InterProcessMutex lock = new InterProcessMutex(curator, lockForEnsureJobPartitions);
        String path = zkRoot + "/" + ZNODE_PARTITIONS;
        try {
            lock.acquire();
            int minDate = 0;
            String forceStartFrom = readForceStartFrom();
            if (forceStartFrom != null) {
                try {
                    minDate = Integer.valueOf(forceStartFrom);
                } catch (Exception ex) {
                    LOG.error("failing converting forceStartFrom znode value to integer with value " + forceStartFrom);
                    throw new IllegalStateException();
                }
            } else {
                boolean pathExists = curator.checkExists().forPath(path) == null ? false : true;
                boolean structureChanged = true;
                if (pathExists) {
                    int currentCount = curator.getChildren().forPath(path).size();
                    if (numTotalPartitions == currentCount) {
                        structureChanged = false;
                        LOG.info("znode partitions structure is unchanged");
                    } else {
                        LOG.info("znode partitions structure is changed, current partition count " + currentCount + ", future count " + numTotalPartitions);
                    }
                }
                if (!structureChanged) {
                    return; // do nothing
                }

                if (pathExists) {
                    List<String> partitions = curator.getChildren().forPath(path);
                    for (String partition : partitions) {
                        String date = new String(curator.getData().forPath(path + "/" + partition), "UTF-8");
                        int tmp = Integer.valueOf(date);
                        if (tmp < minDate) {
                            minDate = tmp;
                        }
                    }
                }

                if (minDate == 0) {
                    minDate = Integer.valueOf(getProcessedDateAfterBackoff(BACKOFF_DAYS));
                }
            }
            rebuildJobPartitions(numTotalPartitions, String.valueOf(minDate));
            deleteForceStartFrom();
        } catch (Exception e) {
            LOG.error("fail building job partitions", e);
            throw new RuntimeException(e);
        } finally {
            try {
                lock.release();
            } catch (Exception e) {
                LOG.error("fail releasing lock", e);
                throw new RuntimeException(e);
            }
        }
    }

    private void rebuildJobPartitions(int numTotalPartitions, String startingDate) throws Exception {
        LOG.info("rebuild job partitions with numTotalPartitions " + numTotalPartitions + " with starting date " + startingDate);
        String path = zkRoot + "/" + ZNODE_PARTITIONS;
        // truncate all existing partitions
        if (curator.checkExists().forPath(path) != null) {
            curator.delete().deletingChildrenIfNeeded().forPath(path);
        }

        for (int i = 0; i < numTotalPartitions; i++) {
            curator.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path + "/" + i, startingDate.getBytes("UTF-8"));
        }
    }

    @Override
    public String readProcessedDate(int partitionId) {
        String path = zkRoot + "/" + ZNODE_PARTITIONS + "/" + partitionId;
        try {
            if (curator.checkExists().forPath(path) != null) {
                return new String(curator.getData().forPath(path), "UTF-8");
            } else {
                return null;
            }
        } catch (Exception e) {
            LOG.error("fail read processed date", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateProcessedDate(int partitionId, String date) {
        String path = zkRoot + "/" + ZNODE_PARTITIONS + "/" + partitionId;
        try {
            if (curator.checkExists().forPath(path) == null) {
                curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, date.getBytes("UTF-8"));
            } else {
                curator.setData().forPath(path, date.getBytes("UTF-8"));
            }
        } catch (Exception e) {
            LOG.error("fail update processed date", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addProcessedJob(String date, String jobId) {
        String path = zkRoot + "/" + ZNODE_JOBS + "/" + date + "/" + jobId;
        try {
            if (curator.checkExists().forPath(path) == null) {
                curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path);
            } else {
                curator.setData().forPath(path);
            }
        } catch (Exception e) {
            LOG.error("fail adding processed jobs", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void truncateProcessedJob(String date) {
        LOG.info("trying to truncate all data for day " + date);
        // we need lock before we do truncate
        String path = zkRoot + "/" + ZNODE_JOBS + "/" + date;
        InterProcessMutex lock = new InterProcessMutex(curator, path);
        try {
            lock.acquire();
            if (curator.checkExists().forPath(path) != null) {
                curator.delete().deletingChildrenIfNeeded().forPath(path);
                LOG.info("really truncated all data for day " + date);
            }

            String jobIdPath = zkRoot + "/" + ZNODE_JOB_IDS + "/" + date;
            if (curator.checkExists().forPath(jobIdPath) != null) {
                curator.delete().deletingChildrenIfNeeded().forPath(jobIdPath);
                LOG.info("really truncated all jobIds for day " + date);
            }
        } catch (Exception e) {
            LOG.error("fail truncating processed jobs", e);
            throw new RuntimeException(e);
        } finally {
            try {
                lock.release();
            } catch (Exception e) {
                LOG.error("fail releasing lock", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public List<String> readProcessedJobs(String date) {
        String path = zkRoot + "/" + ZNODE_JOBS + "/" + date;
        try {
            if (curator.checkExists().forPath(path) != null) {
                return curator.getChildren().forPath(path);
            } else {
                return null;
            }
        } catch (Exception e) {
            LOG.error("fail read processed jobs", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void truncateEverything() {
        String path = zkRoot;
        try {
            if (curator.checkExists().forPath(path) != null) {
                curator.delete().deletingChildrenIfNeeded().forPath(path);
            }
        } catch (Exception ex) {
            LOG.error("fail truncating verything", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public long readProcessedTimeStamp(int partitionId) {
        String path = zkRoot + "/" + ZNODE_PARTITIONS + "/" + partitionId + "/" + ZNODE_TIMESTAMPS;
        try {
            if (curator.checkExists().forPath(path) == null) {
                curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path);
                return 0L;
            } else {
                return Long.parseLong(new String(curator.getData().forPath(path), "UTF-8"));
            }
        } catch (Exception e) {
            LOG.error("fail to read timeStamp for partition " + partitionId, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateProcessedTimeStamp(int partitionId, long timeStamp) {
        String path = zkRoot + "/" + ZNODE_PARTITIONS + "/" + partitionId + "/" + ZNODE_TIMESTAMPS;
        try {
            if (curator.checkExists().forPath(path) == null) {
                curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path);
            }

            curator.setData().forPath(path, (timeStamp + "").getBytes("UTF-8"));
        } catch (Exception e) {
            LOG.error("fail to update timeStamp for partition " + partitionId, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Pair<String, String>> getProcessedJobs(String date) {
        List<Pair<String, String>> result = new ArrayList<>();
        String path = zkRoot + "/" + ZNODE_JOB_IDS + "/" + date;
        try {
            if (curator.checkExists().forPath(path) != null) {
                List<String> jobs = curator.getChildren().forPath(path);
                for (String job : jobs) {
                    String jobPath = path + "/" + job;
                    String status = new String(curator.getData().forPath(jobPath), "UTF-8");
                    result.add(Pair.of(job, status));
                }
            }
        } catch (Exception e) {
            LOG.error("fail read processed jobs", e);
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public void updateProcessedJob(String date, String jobId, String status) {
        String path = zkRoot + "/" + ZNODE_JOB_IDS + "/" + date + "/" + jobId;
        try {
            if (curator.checkExists().forPath(path) == null) {
                curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path);
            }
            curator.setData().forPath(path, status.getBytes("UTF-8"));
        } catch (Exception e) {
            LOG.error("fail adding processed jobs", e);
            throw new RuntimeException(e);
        }
    }
}
