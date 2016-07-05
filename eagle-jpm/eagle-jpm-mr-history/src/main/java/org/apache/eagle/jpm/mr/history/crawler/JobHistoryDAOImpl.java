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

package org.apache.eagle.jpm.mr.history.crawler;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.eagle.jpm.util.HDFSUtil;
import org.apache.eagle.jpm.mr.history.common.JHFConfigManager.JobHistoryEndpointConfig;

public class JobHistoryDAOImpl extends AbstractJobHistoryDAO {
    private static final Logger LOG = LoggerFactory.getLogger(JobHistoryDAOImpl.class);

    private Configuration m_conf = new Configuration();

    private FileSystem m_hdfs;

    public JobHistoryDAOImpl(JobHistoryEndpointConfig endpointConfig) throws Exception {
        super(endpointConfig.basePath, endpointConfig.pathContainsJobTrackerName, endpointConfig.jobTrackerName);
        this.m_conf.set("fs.defaultFS", endpointConfig.nnEndpoint);
        this.m_conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        if (!endpointConfig.principal.equals("")) {
            this.m_conf.set("hdfs.kerberos.principal", endpointConfig.principal);
            this.m_conf.set("hdfs.keytab.file", endpointConfig.keyTab);
        }
        LOG.info("file system:" + endpointConfig.nnEndpoint);
        m_hdfs = HDFSUtil.getFileSystem(m_conf);
    }

    @Override
    public void freshFileSystem() throws Exception {
        try {
            m_hdfs.close();
        } catch (Exception e) {

        } finally {
            m_hdfs = HDFSUtil.getFileSystem(m_conf);
        }
    }

    @Override
    public String calculateJobTrackerName(String basePath) throws Exception {
        String latestJobTrackerName = null;
        try {
            Path hdfsFile = new Path(basePath);
            FileStatus[] files = m_hdfs.listStatus(hdfsFile);

            // Sort by modification time as order of desc
            Arrays.sort(files, new Comparator<FileStatus>() {
                @Override
                public int compare(FileStatus o1, FileStatus o2) {
                    long comp = parseJobTrackerNameTimestamp(o1.getPath().toString()) - parseJobTrackerNameTimestamp(o2.getPath().toString());
                    if (comp > 0l) {
                        return -1;
                    } else if (comp < 0l) {
                        return 1;
                    }
                    return 0;
                }
            });

            for (FileStatus fs : files) {
                // back-compatible with hadoop 0.20
                // pick the first directory file which should be the latest modified.
                if (fs.isDir()) {
                    latestJobTrackerName = fs.getPath().getName();
                    break;
                }
            }
        } catch(Exception ex) {
            LOG.error("fail read job tracker name " + basePath, ex);
            throw ex;
        }
        return latestJobTrackerName == null ? "" : latestJobTrackerName;
    }

    @Override
    public List<String> readSerialNumbers(int year, int month, int day) throws Exception {
        List<String> serialNumbers = new ArrayList<>();
        String dailyPath = buildWholePathToYearMonthDay(year, month, day);
        LOG.info("crawl serial numbers under one day : " + dailyPath);
        try {
            Path hdfsFile = new Path(dailyPath);
            FileStatus[] files = m_hdfs.listStatus(hdfsFile);
            for (FileStatus fs : files) {
                if (fs.isDir()) {
                    serialNumbers.add(fs.getPath().getName());
                }
            }
        } catch (java.io.FileNotFoundException ex) {
            LOG.warn("continue to crawl with failure to find file " + dailyPath);
            LOG.debug("continue to crawl with failure to find file " + dailyPath, ex);
            // continue to execute
            return serialNumbers;
        } catch (Exception ex) {
            LOG.error("fail reading serial numbers under one day " + dailyPath, ex);
            throw ex;
        }
        StringBuilder sb = new StringBuilder();
        for (String sn : serialNumbers) {
            sb.append(sn);sb.append(",");
        }
        LOG.info("crawled serialNumbers: " + sb);
        return serialNumbers;
    }

    @SuppressWarnings("deprecation")
    @Override
    public List<Pair<Long, String> > readFileNames(int year, int month, int day, int serialNumber) throws Exception {
        LOG.info("crawl file names under one serial number : " + year + "/" + month + "/" + day + ":" + serialNumber);
        List<Pair<Long, String> > jobFileNames = new ArrayList<>();
        String serialPath = buildWholePathToSerialNumber(year, month, day, serialNumber);
        try {
            Path hdfsFile = new Path(serialPath);
            // filter those files which is job configuration file in xml format
            FileStatus[] files = m_hdfs.listStatus(hdfsFile, new PathFilter(){
                @Override
                public boolean accept(Path path){
                    if (path.getName().endsWith(".xml"))
                        return false;
                    return true;
                }
            });
            for (FileStatus fs : files) {
                if (!fs.isDir()) {
                    jobFileNames.add(Pair.of(fs.getModificationTime(), fs.getPath().getName()));
                }
            }
            if (LOG.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                for (Pair<Long, String> sn : jobFileNames) {
                    sb.append(sn.getRight());sb.append(",");
                }
                LOG.debug("crawled: " + sb);
            }
        } catch (Exception ex) {
            LOG.error("fail reading job history file names under serial number " + serialPath, ex);
            throw ex;
        }
        return jobFileNames;
    }

    /**
     * it's the responsibility of caller to close input stream
     */
    @Override
    public InputStream getJHFFileContentAsStream(int year, int month, int day, int serialNumber, String jobHistoryFileName) throws Exception {
        String path = buildWholePathToJobHistoryFile(year, month, day, serialNumber, jobHistoryFileName);
        LOG.info("Read job history file: " + path);
        try {
            Path hdfsFile = new Path(path);
            return m_hdfs.open(hdfsFile);
        } catch(Exception ex) {
            LOG.error("fail getting hdfs file inputstream " + path, ex);
            throw ex;
        }
    }

    /**
     * it's the responsibility of caller to close input stream
     */
    @Override
    public InputStream getJHFConfContentAsStream(int year, int month, int day, int serialNumber, String jobHistoryFileName) throws Exception {
        String path = buildWholePathToJobConfFile(year, month, day, serialNumber,jobHistoryFileName);
        if (path  == null) return null;

        LOG.info("Read job conf file: " + path);
        try {
            Path hdfsFile = new Path(path);
            return m_hdfs.open(hdfsFile);
        } catch(Exception ex) {
            LOG.error("fail getting job configuration input stream from " + path, ex);
            throw ex;
        }
    }
}
