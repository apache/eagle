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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * job history is the resource.
 */
public abstract class AbstractJobHistoryDAO implements JobHistoryLCM {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJobHistoryDAO.class);

    private static final String YEAR_URL_FORMAT = "/%4d";
    private static final String MONTH_URL_FORMAT = "/%02d";
    private static final String DAY_URL_FORMAT = "/%02d";
    private static final String YEAR_MONTH_DAY_URL_FORMAT = YEAR_URL_FORMAT + MONTH_URL_FORMAT + DAY_URL_FORMAT;
    protected static final String SERIAL_URL_FORMAT = "/%06d";
    protected static final String FILE_URL_FORMAT = "/%s";
    private static final Pattern JOBTRACKERNAME_PATTERN = Pattern.compile("^.*_(\\d+)_$");
    protected static final Pattern JOBID_PATTERN = Pattern.compile("job_\\d+_\\d+");

    protected final String basePath;
    protected volatile String jobTrackerName;

    public  static final String JOB_CONF_POSTFIX = "_conf.xml";

    private static final Timer timer = new Timer(true);
    private static final long JOB_TRACKER_SYNC_DURATION = 10 * 60 * 1000; // 10 minutes

    private boolean pathContainsJobTrackerName;

    public AbstractJobHistoryDAO(String basePath, boolean pathContainsJobTrackerName, String startingJobTrackerName) throws Exception {
        this.basePath = basePath;
        this.pathContainsJobTrackerName = pathContainsJobTrackerName;
        jobTrackerName = startingJobTrackerName;
        if (this.pathContainsJobTrackerName) {
            if (startingJobTrackerName == null || startingJobTrackerName.isEmpty()) {
                throw new IllegalStateException("startingJobTrackerName should not be null or empty");
            }
            // start background thread to check what is current job tracker
            startThread(this.basePath);
        }
    }

    protected String buildWholePathToYearMonthDay(int year, int month, int day) {
        StringBuilder sb = new StringBuilder();
        sb.append(basePath);
        if (!pathContainsJobTrackerName && jobTrackerName != null && !jobTrackerName.isEmpty()) {
            sb.append("/");
            sb.append(jobTrackerName);
        }
        sb.append(String.format(YEAR_MONTH_DAY_URL_FORMAT, year, month, day));
        return sb.toString();
    }

    protected String buildWholePathToSerialNumber(int year, int month, int day, int serialNumber) {
        String wholePathToYearMonthDay = buildWholePathToYearMonthDay(year, month, day);
        StringBuilder sb = new StringBuilder();
        sb.append(wholePathToYearMonthDay);
        sb.append(String.format(SERIAL_URL_FORMAT, serialNumber));
        return sb.toString();
    }

    protected String buildWholePathToJobHistoryFile(int year, int month, int day, int serialNumber, String jobHistoryFileName) {
        String wholePathToJobHistoryFile = buildWholePathToSerialNumber(year, month, day, serialNumber);
        StringBuilder sb = new StringBuilder();
        sb.append(wholePathToJobHistoryFile);
        sb.append(String.format(FILE_URL_FORMAT, jobHistoryFileName));
        return sb.toString();
    }


    protected String buildWholePathToJobConfFile(int year, int month, int day, int serialNumber,String jobHistFileName) {
        Matcher matcher = JOBID_PATTERN.matcher(jobHistFileName);
        if (matcher.find()) {
            String wholePathToJobConfFile = buildWholePathToSerialNumber(year, month, day, serialNumber);
            StringBuilder sb = new StringBuilder();
            sb.append(wholePathToJobConfFile);
            sb.append("/");
            sb.append(String.format(FILE_URL_FORMAT, matcher.group()));
            sb.append(JOB_CONF_POSTFIX);
            return sb.toString();
        }
        LOG.warn("Illegal job history file name: " + jobHistFileName);
        return null;
    }

    private void startThread(final String basePath) throws Exception {
        LOG.info("start an every-" + JOB_TRACKER_SYNC_DURATION / (60 * 1000) + "min timer task to check current jobTrackerName in background");
        // Automatically update current job tracker name in background every 30 minutes
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    LOG.info("regularly checking current jobTrackerName in background");
                    final String _jobTrackerName = calculateJobTrackerName(basePath);
                    if (_jobTrackerName != null && !_jobTrackerName.equals(jobTrackerName)) {
                        LOG.info("jobTrackerName changed from " + jobTrackerName + " to " + _jobTrackerName);
                        jobTrackerName = _jobTrackerName;
                    }
                    LOG.info("Current jobTrackerName is: " + jobTrackerName);
                } catch (Exception e) {
                    LOG.error("failed to figure out current job tracker name that is not configured due to: " + e.getMessage(), e);
                } catch (Throwable t) {
                    LOG.error("failed to figure out current job tracker name that is not configured due to: " + t.getMessage(), t);
                }
            }
        }, JOB_TRACKER_SYNC_DURATION, JOB_TRACKER_SYNC_DURATION);
    }


    @Override
    public void readFileContent(int year, int month, int day, int serialNumber, String jobHistoryFileName, JHFInputStreamCallback reader) throws Exception {
        InputStream downloadIs;
        try {
            downloadIs = getJHFFileContentAsStream(year, month, day, serialNumber, jobHistoryFileName);
        } catch (FileNotFoundException ex) {
            LOG.error("job history file not found " + jobHistoryFileName + ", ignore and will NOT process any more");
            return;
        }

        InputStream downloadJobConfIs = null;
        try {
            downloadJobConfIs = getJHFConfContentAsStream(year, month, day, serialNumber, jobHistoryFileName);
        } catch (FileNotFoundException ex) {
            LOG.warn("job configuration file of " + jobHistoryFileName + " not found , ignore and use empty configuration");
        }

        org.apache.hadoop.conf.Configuration conf = null;

        if (downloadJobConfIs != null) {
            conf = new org.apache.hadoop.conf.Configuration();
            conf.addResource(downloadJobConfIs);
        }

        try {
            if (downloadIs != null) {
                reader.onInputStream(downloadIs, conf);
            }
        } catch (Exception ex) {
            LOG.error("fail reading job history file", ex);
            throw ex;
        } catch (Throwable t) {
            LOG.error("fail reading job history file", t);
            throw new Exception(t);
        } finally {
            try {
                if (downloadJobConfIs != null) {
                    downloadJobConfIs.close();
                }
                if (downloadIs != null) {
                    downloadIs.close();
                }
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }


    protected static long parseJobTrackerNameTimestamp(String jtname) {
        Matcher matcher = JOBTRACKERNAME_PATTERN.matcher(jtname);
        if (matcher.find()) {
            return Long.parseLong(matcher.group(1));
        }
        LOG.warn("invalid job tracker name: " + jtname);
        return -1;
    }
}

