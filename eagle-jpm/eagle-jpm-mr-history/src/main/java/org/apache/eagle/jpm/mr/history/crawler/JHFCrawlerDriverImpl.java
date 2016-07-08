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
import org.apache.eagle.jpm.mr.history.common.JHFConfigManager;
import org.apache.eagle.jpm.mr.history.storm.JobIdFilter;
import org.apache.eagle.jpm.mr.history.zkres.JobHistoryZKStateLCM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * single thread crawling per driver
 * multiple drivers can achieve parallelism
 *
 */
public class JHFCrawlerDriverImpl implements JHFCrawlerDriver {
    private static final Logger LOG = LoggerFactory.getLogger(JHFCrawlerDriverImpl.class);

    private final static int SLEEP_SECONDS_WHILE_QUEUE_IS_EMPTY = 10;
    private final static String FORMAT_JOB_PROCESS_DATE = "%4d%02d%02d";
    private final static Pattern PATTERN_JOB_PROCESS_DATE = Pattern.compile("([0-9]{4})([0-9]{2})([0-9]{2})");

    private static final int INITIALIZED = 0x0;
    private static final int TODAY = 0x1;
    private static final int BEFORETODAY = 0x10;
    private final int PROCESSED_JOB_KEEP_DAYS = 5;

    private int m_flag = INITIALIZED; // 0 not set, 1 TODAY, 2 BEFORETODAY
    private Deque<Pair<Long, String> > m_processQueue = new LinkedList<>();
    private Set<String> m_processedJobFileNames = new HashSet<>();

    private final JobProcessDate m_proceeDate = new JobProcessDate();
    private boolean m_dryRun;
    private JHFInputStreamCallback m_reader;
    protected boolean m_zeroBasedMonth = true;

    private JobHistoryZKStateLCM m_zkStatelcm;
    private JobHistoryLCM m_jhfLCM;
    private JobIdFilter m_jobFilter;
    private int m_partitionId;
    private TimeZone m_timeZone;

    public JHFCrawlerDriverImpl(JHFConfigManager.JobHistoryEndpointConfig jobHistoryConfig,
                                JHFConfigManager.ControlConfig controlConfig, JHFInputStreamCallback reader,
                                JobHistoryZKStateLCM zkStateLCM,
                                JobHistoryLCM historyLCM, JobIdFilter jobFilter, int partitionId) throws Exception {
        this.m_zeroBasedMonth = controlConfig.zeroBasedMonth;
        this.m_dryRun = controlConfig.dryRun;
        if (this.m_dryRun)  LOG.info("this is a dry run");
        this.m_reader = reader;
        m_jhfLCM = historyLCM;//new JobHistoryDAOImpl(jobHistoryConfig);
        this.m_zkStatelcm = zkStateLCM;
        this.m_partitionId = partitionId;
        this.m_jobFilter = jobFilter;
        m_timeZone = TimeZone.getTimeZone(controlConfig.timeZone);
    }

    /**
     * <br>
     * 1. if queue is not empty <br>
     * 1.1 dequeue and process one job file <br>
     * 1.2 store processed job file and also cache it to processedJobFileNames
     * 2. if queue is empty <br>
     * 2.0 if flag is BEFORETODAY, then write currentProcessedDate to jobProcessedDate as this day's data are all processed <br>
     * 2.1 crawl that day's job file list <br>
     * 2.2 filter out those jobID which are in _processedJobIDs keyed by
     * currentProcessedDate <br>
     * 2.3 put available file list to processQueue and then go to step 1
     */
    @Override
    public long crawl() throws Exception {
        LOG.info("queue size is " + m_processQueue.size());
        while (m_processQueue.isEmpty()) {
            // read lastProcessedDate only when it's initialized
            if (m_flag == INITIALIZED) {
                readAndCacheLastProcessedDate();
            }
            if (m_flag == BEFORETODAY) {
                updateProcessDate();
                clearProcessedJobFileNames();
            }
            if (m_flag != TODAY) { // advance one day if initialized or BEFORE today
                advanceOneDay();
            }

            if (isToday()) {
                m_flag = TODAY;
            } else {
                m_flag = BEFORETODAY;
            }

            List<String> serialNumbers = m_jhfLCM.readSerialNumbers(this.m_proceeDate.year, getActualMonth(m_proceeDate.month), this.m_proceeDate.day);
            List<Pair<Long, String> > allJobHistoryFiles = new LinkedList<>();
            for (String serialNumber : serialNumbers) {
                List<Pair<Long, String> > jobHistoryFiles = m_jhfLCM.readFileNames(
                        this.m_proceeDate.year,
                        getActualMonth(m_proceeDate.month),
                        this.m_proceeDate.day,
                        Integer.parseInt(serialNumber));
                LOG.info("total number of job history files " + jobHistoryFiles.size());
                for (Pair<Long, String> jobHistoryFile : jobHistoryFiles) {
                    if (m_jobFilter.accept(jobHistoryFile.getRight()) && !fileProcessed(jobHistoryFile.getRight())) {
                        allJobHistoryFiles.add(jobHistoryFile);
                    }
                }
                jobHistoryFiles.clear();
                LOG.info("after filtering, number of job history files " + m_processQueue.size());
            }

            Collections.sort(allJobHistoryFiles,
                    new Comparator<Pair<Long, String>>() {
                        @Override
                        public int compare(Pair<Long, String> o1, Pair<Long, String> o2) {
                            if (o1.getLeft() > o2.getLeft()) return 1;
                            else if (o1.getLeft() == o2.getLeft()) return 0;
                            else return -1;
                        }
                    }
            );
            for (Pair<Long, String> jobHistoryFile : allJobHistoryFiles) {
                m_processQueue.add(jobHistoryFile);
            }

            allJobHistoryFiles.clear();

            if (m_processQueue.isEmpty()) {
                Thread.sleep(SLEEP_SECONDS_WHILE_QUEUE_IS_EMPTY * 1000);
            } else {
                LOG.info("queue size after populating is now : " + m_processQueue.size());
            }
        }
        // start to process job history file
        Pair<Long, String> item = m_processQueue.pollFirst();
        String jobHistoryFile = item.getRight();
        Long modifiedTime = item.getLeft();
        if (jobHistoryFile == null) { // terminate this round of crawling when the queue is empty
            LOG.info("process queue is empty, ignore this round");
            return -1;
        }
        // get serialNumber from job history file name
        Pattern p = Pattern.compile("^job_[0-9]+_([0-9]+)[0-9]{3}[_-]{1}");
        Matcher m = p.matcher(jobHistoryFile);
        String serialNumber;
        if (m.find()) {
            serialNumber = m.group(1);
        } else {
            LOG.warn("illegal job history file name : " + jobHistoryFile);
            return -1;
        }
        if (!m_dryRun) {
            m_jhfLCM.readFileContent(
                    m_proceeDate.year,
                    getActualMonth(m_proceeDate.month),
                    m_proceeDate.day,
                    Integer.valueOf(serialNumber),
                    jobHistoryFile,
                    m_reader);
        }
        m_zkStatelcm.addProcessedJob(String.format(FORMAT_JOB_PROCESS_DATE,
                this.m_proceeDate.year,
                this.m_proceeDate.month + 1,
                this.m_proceeDate.day),
                jobHistoryFile);
        m_processedJobFileNames.add(jobHistoryFile);

        return modifiedTime;
    }

    private void updateProcessDate() throws Exception {
        String line = String.format(FORMAT_JOB_PROCESS_DATE, this.m_proceeDate.year,
                this.m_proceeDate.month + 1, this.m_proceeDate.day);
        m_zkStatelcm.updateProcessedDate(m_partitionId, line);
    }

    private int getActualMonth(int month){
        return m_zeroBasedMonth ? m_proceeDate.month : m_proceeDate.month + 1;
    }

    private static class JobProcessDate {
        public int year;
        public int month; // 0 based month
        public int day;
    }

    private void clearProcessedJobFileNames() {
        m_processedJobFileNames.clear();
    }

    private void readAndCacheLastProcessedDate() throws Exception {
        String lastProcessedDate = m_zkStatelcm.readProcessedDate(m_partitionId);
        Matcher m = PATTERN_JOB_PROCESS_DATE.matcher(lastProcessedDate);
        if (m.find() && m.groupCount() == 3) {
            this.m_proceeDate.year = Integer.parseInt(m.group(1));
            this.m_proceeDate.month = Integer.parseInt(m.group(2)) - 1; // zero based month
            this.m_proceeDate.day = Integer.parseInt(m.group(3));
        } else {
            throw new IllegalStateException("job lastProcessedDate must have format YYYYMMDD " + lastProcessedDate);
        }

        GregorianCalendar cal = new GregorianCalendar(m_timeZone);
        cal.set(this.m_proceeDate.year, this.m_proceeDate.month, this.m_proceeDate.day, 0, 0, 0);
        cal.add(Calendar.DATE, 1);
        List<String> list = m_zkStatelcm.readProcessedJobs(String.format(FORMAT_JOB_PROCESS_DATE, cal.get(Calendar.YEAR),
                cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH)));
        if (list != null) {
            this.m_processedJobFileNames = new HashSet<>(list);
        }
    }

    private void advanceOneDay() throws Exception {
        GregorianCalendar cal = new GregorianCalendar(m_timeZone);
        cal.set(this.m_proceeDate.year, this.m_proceeDate.month, this.m_proceeDate.day, 0, 0, 0);
        cal.add(Calendar.DATE, 1);
        this.m_proceeDate.year = cal.get(Calendar.YEAR);
        this.m_proceeDate.month = cal.get(Calendar.MONTH);
        this.m_proceeDate.day = cal.get(Calendar.DAY_OF_MONTH);

        try {
            clearProcessedJob(cal);
        } catch (Exception e) {
            LOG.error("failed to clear processed job ", e);
        }

    }

    private void clearProcessedJob(Calendar cal) {
        // clear all already processed jobs some days before current processing date (PROCESSED_JOB_KEEP_DAYS)
        cal.add(Calendar.DATE, -1 - PROCESSED_JOB_KEEP_DAYS);
        String line = String.format(FORMAT_JOB_PROCESS_DATE, cal.get(Calendar.YEAR),
                cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH));
        m_zkStatelcm.truncateProcessedJob(line);
    }

    private boolean isToday() {
        GregorianCalendar today = new GregorianCalendar(m_timeZone);

        if (today.get(Calendar.YEAR) == this.m_proceeDate.year
                && today.get(Calendar.MONTH) == this.m_proceeDate.month
                && today.get(Calendar.DAY_OF_MONTH) == this.m_proceeDate.day)
            return true;

        return false;
    }

    /**
     * check if this file was already processed
     *
     * @param fileName
     * @return
     */
    private boolean fileProcessed(String fileName) {
        if (m_processedJobFileNames.contains(fileName))
            return true;
        return false;
    }
}
