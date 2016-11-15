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

public class JobHistoryZKStateManager {
    public static final Logger LOG = LoggerFactory.getLogger(JobHistoryZKStateManager.class);
    private String zkRoot;
    private CuratorFramework curator;
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
            LOG.warn("curator already started, {}", e);
        }
    }

    public void close() {
        curator.close();
        curator = null;
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
    public void ensureJobPartition(int partitionId, int numTotalPartitions) {
        // lock before rebuild job partitions
        String path = zkRoot + "/" + ZNODE_PARTITIONS;
        try {
            boolean partitionPathExists = curator.checkExists().forPath(path + "/" + partitionId) != null;
            if (partitionPathExists) {
                LOG.info("partition path {} exists", path + "/" + partitionId);
                List<String> partitions = curator.getChildren().forPath(path);
                if (partitions.size() > numTotalPartitions && numTotalPartitions == partitionId + 1) {
                    //last partition delete needless partitions
                    for (String partition : partitions) {
                        if (Integer.parseInt(partition) > partitionId) {
                            curator.delete().deletingChildrenIfNeeded().forPath(path + "/" + partition);
                            LOG.info("delete partition {}", path + "/" + partition);
                        }
                    }
                }
                return;
            }

            int minDate = 0;
            boolean pathExists = curator.checkExists().forPath(path) != null;
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

            rebuildJobPartition(partitionId, String.valueOf(minDate));
        } catch (Exception e) {
            LOG.error("fail building job partitions", e);
            throw new RuntimeException(e);
        }
    }

    private void rebuildJobPartition(int partitionId, String startingDate) throws Exception {
        LOG.info("create job partition " + partitionId + " with starting date " + startingDate);
        String path = zkRoot + "/" + ZNODE_PARTITIONS + "/" + partitionId;

        curator.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT).forPath(path, startingDate.getBytes("UTF-8"));

        updateProcessedTimeStamp(partitionId, 0L);
    }

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
