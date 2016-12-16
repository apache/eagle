/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.mr.history;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.apache.commons.lang.time.StopWatch;
import org.apache.eagle.app.service.ApplicationEmailService;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.common.mail.AlertEmailConstants;
import org.apache.eagle.common.mail.AlertEmailContext;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.service.client.EagleServiceClientException;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.eagle.common.config.EagleConfigConstants.SERVICE_HOST;
import static org.apache.eagle.common.config.EagleConfigConstants.SERVICE_PORT;

public class MRHistoryJobDailyReporter extends AbstractScheduledService {
    private static final Logger LOG = LoggerFactory.getLogger(MRHistoryJobDailyReporter.class);

    private static final String DAILY_SENT_HOUROFDAY = "application.dailyJobReport.reportHourTime";
    private static final String DAILY_SENT_PERIOD = "application.dailyJobReport.reportPeriodInHour";
    private static final String NUM_TOP_USERS  = "application.dailyJobReport.numTopUsers";
    private static final String JOB_OVERTIME_LIMIT_HOUR  = "application.dailyJobReport.jobOvertimeLimitInHour";

    public static final String SERVICE_PATH = "application.dailyJobReport";
    protected static final String APP_TYPE = "MR_HISTORY_JOB_APP";
    protected static final String TIMEZONE_PATH = "service.timezone";

    // alert context keys
    protected static final String NUM_TOP_USERS_KEY = "numTopUsers";
    protected static final String JOB_OVERTIME_LIMIT_KEY = "jobOvertimeLimit";
    protected static final String ALERT_TITLE_KEY = "alertTitle";
    protected static final String REPORT_RANGE_KEY = "reportRange";
    protected static final String SUMMARY_INFO_KEY = "summaryInfo";
    protected static final String FAILED_JOB_USERS_KEY = "failedJobUsers";
    protected static final String SUCCEEDED_JOB_USERS_KEY = "succeededJobUsers";
    protected static final String FINISHED_JOB_USERS_KEY = "finishedJobUsers";
    protected static final String EAGLE_JOB_LINK_KEY = "eagleJobLink";

    // queries
    private static final String STATUS_QUERY = "%s[@site=\"%s\" and @endTime<=%s]<@currentState>{count}.{count desc}";
    private static final String FAILED_JOBS_QUERY = "%s[@site=\"%s\" and @currentState=\"FAILED\" and @endTime<=%s]<@user>{count}.{count desc}";
    private static final String SUCCEEDED_JOB_QUERY = "%s[@site=\"%s\" and @currentState=\"SUCCEEDED\" and @durationTime>%s and @endTime<=%s]<@user>{count}.{count desc}";
    private static final String FINISHED_JOB_QUERY = "%s[@site=\"%s\" and @endTime<=%s]<@user>{count}.{count desc}";

    private Config config;
    private IEagleServiceClient client;
    private ApplicationEntityService applicationResource;
    private ApplicationEmailService emailService;
    private boolean isDailySent = false;
    private long lastSentTime;

    private int dailySentHour;
    private int dailySentPeriod;
    private int numTopUsers = 10;
    private int jobOvertimeLimit = 6;

    // scheduler
    private int initialDelayMin = 10;
    private int periodInMin = 60;
    private TimeZone timeZone;

    @Inject
    public MRHistoryJobDailyReporter(Config config, ApplicationEntityService applicationEntityService) {
        this.timeZone = TimeZone.getTimeZone(config.getString(TIMEZONE_PATH));

        if (config.hasPath(SERVICE_PATH) && config.hasPath(AlertEmailConstants.EAGLE_APPLICATION_EMAIL_SERVICE)) {
            this.emailService = new ApplicationEmailService(config, SERVICE_PATH);
        }
        if (config.hasPath(DAILY_SENT_HOUROFDAY)) {
            this.dailySentHour = config.getInt(DAILY_SENT_HOUROFDAY);
        }
        if (config.hasPath(DAILY_SENT_PERIOD)) {
            this.dailySentPeriod = config.getInt(DAILY_SENT_PERIOD);
        }
        if (config.hasPath(NUM_TOP_USERS)) {
            this.numTopUsers = config.getInt(NUM_TOP_USERS);
        }
        if (config.hasPath(JOB_OVERTIME_LIMIT_HOUR)) {
            this.jobOvertimeLimit = config.getInt(JOB_OVERTIME_LIMIT_HOUR);
        }
        this.config = config;
        this.applicationResource = applicationEntityService;
    }

    private boolean isSentHour(int currentHour) {
        return Math.abs(currentHour - dailySentHour) % dailySentPeriod == 0;
    }

    private Collection<String> loadSites(String appType) {
        Set<String> sites = new HashSet<>();
        Collection<ApplicationEntity> apps = applicationResource.findAll();
        for (ApplicationEntity app : apps) {
            if (app.getDescriptor().getType().equalsIgnoreCase(appType) && app.getStatus().equals(ApplicationEntity.Status.RUNNING)) {
                sites.add(app.getSite().getSiteId());
            }
        }
        LOG.info("Detected {} sites where MR_HISTORY_JOB_APP is Running: {}", sites.size(), sites);
        return sites;
    }

    @Override
    protected void runOneIteration() throws Exception {
        GregorianCalendar calendar = new GregorianCalendar(timeZone);
        int currentHour = calendar.get(Calendar.HOUR_OF_DAY);
        long currentTimestamp = calendar.getTimeInMillis();
        if (!isSentHour(currentHour)) {
            isDailySent = false;
        } else if (!isDailySent) {
            isDailySent = true;
            LOG.info("last job report time is {} %s", DateTimeUtil.millisecondsToHumanDateWithSeconds(lastSentTime), timeZone.getID());
            try {
                Collection<String> sites = loadSites(APP_TYPE);
                if (sites == null || sites.isEmpty()) {
                    LOG.warn("application MR_HISTORY_JOB_APP does not run on any sites!");
                    return;
                }
                for (String site : sites) {
                    int reportHour = currentHour / dailySentPeriod * dailySentPeriod;
                    calendar.set(Calendar.HOUR_OF_DAY, reportHour);
                    long endTime = calendar.getTimeInMillis() / DateTimeUtil.ONEHOUR * DateTimeUtil.ONEHOUR;
                    long startTime = endTime - DateTimeUtil.ONEHOUR * dailySentPeriod;
                    String subject = buildAlertSubject(site, startTime, endTime);
                    Map<String, Object> alertData = buildAlertData(site, startTime, endTime);
                    sendByEmailWithSubject(alertData, subject);
                }
            } catch (Exception ex) {
                LOG.error("Fail to get job summery info due to {}", ex.getMessage(), ex);
            }
            lastSentTime = currentTimestamp;
        }
    }

    protected void sendByEmail(Map<String, Object> alertData) {
        emailService.onAlert(alertData);
    }

    protected void sendByEmailWithSubject(Map<String, Object> alertData, String subject) {
        AlertEmailContext alertContext = emailService.buildEmailContext(subject);
        emailService.onAlert(alertContext, alertData);
    }

    protected String buildAlertSubject(String site, long startTime, long endTime) {
        String subjectFormat = "[%s] Job Report by %s";
        String date = DateTimeUtil.format(endTime, "yyyyMMdd HH:mm");
        //String startHour = DateTimeUtil.format(startTime, "HH:mm");
        //String endHour = DateTimeUtil.format(endTime, "kk:mm");
        return String.format(subjectFormat, site.toUpperCase(), date);
    }

    private Map<String, Object> buildAlertData(String site, long startTime, long endTime) {
        StopWatch watch = new StopWatch();
        Map<String, Object> data = new HashMap<>();
        this.client = new EagleServiceClientImpl(config);
        String startTimeStr = DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime);
        String endTimeStr = DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime);
        LOG.info("Going to report job summery info for site {} from {} to {}", site, startTimeStr, endTimeStr);
        try {
            watch.start();
            data.putAll(buildJobSummery(site, startTime, endTime));
            data.put(NUM_TOP_USERS_KEY, numTopUsers);
            data.put(JOB_OVERTIME_LIMIT_KEY, jobOvertimeLimit);
            data.put(ALERT_TITLE_KEY, String.format("[%s] Job Report for 12 Hours", site.toUpperCase()));
            data.put(REPORT_RANGE_KEY, String.format("%s ~ %s %s", startTimeStr, endTimeStr, DateTimeUtil.CURRENT_TIME_ZONE.getID()));
            data.put(EAGLE_JOB_LINK_KEY, String.format("http://%s:%d/#/site/%s/jpm/list?startTime=%s&endTime=%s",
                    config.getString(SERVICE_HOST), config.getInt(SERVICE_PORT), site, startTimeStr, endTimeStr));
            watch.stop();
            LOG.info("Fetching DailyJobReport tasks {} seconds", watch.getTime() / DateTimeUtil.ONESECOND);
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                LOG.info("fail to close eagle service client");
            }
        }
        return data;
    }

    private Map<String, Object> buildJobSummery(String site, long startTime, long endTime) {
        Map<String, Object> data = new HashMap<>();

        String query = String.format(STATUS_QUERY, Constants.JPA_JOB_EXECUTION_SERVICE_NAME, site, endTime);
        Map<String, Long> jobSummery = queryGroupByMetrics(query, startTime, endTime, Integer.MAX_VALUE);
        if (jobSummery == null || jobSummery.isEmpty()) {
            LOG.warn("Result set is empty for query={}", query);
            return data;
        }
        Long totalJobs = jobSummery.values().stream().reduce((a, b) -> a + b).get();
        String finishedJobQuery = String.format(FINISHED_JOB_QUERY, Constants.JPA_JOB_EXECUTION_SERVICE_NAME, site, endTime);
        String failedJobQuery = String.format(FAILED_JOBS_QUERY, Constants.JPA_JOB_EXECUTION_SERVICE_NAME, site, endTime);
        String succeededJobQuery = String.format(SUCCEEDED_JOB_QUERY, Constants.JPA_JOB_EXECUTION_SERVICE_NAME, site, jobOvertimeLimit * DateTimeUtil.ONEHOUR, endTime);
        data.put(SUMMARY_INFO_KEY, processResult(jobSummery, totalJobs));
        data.put(FAILED_JOB_USERS_KEY, buildJobSummery(failedJobQuery, startTime, endTime, jobSummery.get(Constants.JobState.FAILED.toString())));
        data.put(SUCCEEDED_JOB_USERS_KEY, buildJobSummery(succeededJobQuery, startTime, endTime, jobSummery.get(Constants.JobState.SUCCEEDED.toString())));
        data.put(FINISHED_JOB_USERS_KEY, buildJobSummery(finishedJobQuery, startTime, endTime, totalJobs));

        return data;
    }

    private List<JobSummaryInfo> buildJobSummery(String query,  long startTime, long endTime, long totalJobs) {
        Map<String, Long> jobUsers = queryGroupByMetrics(query, startTime, endTime, numTopUsers);
        if (jobUsers == null || jobUsers.isEmpty()) {
            LOG.warn("Result set is empty for query={}", query);
            return null;
        }
        return processResult(jobUsers, totalJobs);
    }

    private List<JobSummaryInfo> processResult(Map<String, Long> parsedResult, long totalJobs) {
        List<JobSummaryInfo> summaryInfoList = new ArrayList<>();
        for (Map.Entry<String, Long> entry : parsedResult.entrySet()) {
            JobSummaryInfo summaryInfo = new JobSummaryInfo();
            summaryInfo.key = entry.getKey();
            summaryInfo.numOfJobs = entry.getValue();
            summaryInfo.ratio = Double.parseDouble(String.format("%.2f", summaryInfo.numOfJobs * 100d / totalJobs));
            summaryInfoList.add(summaryInfo);
        }
        return summaryInfoList;
    }

    private Map<String, Long> parseQueryResult(List<Map<List<String>, List<Double>>> result, int limit) {
        Map<String, Long> stateCount = new LinkedHashMap<>();
        for (Map<List<String>, List<Double>> map : result) {
            if (stateCount.size() >= limit) {
                break;
            }
            String key = String.valueOf(map.get("key").get(0));
            Long value = map.get("value").get(0).longValue();
            stateCount.put(key, value);
        }
        return stateCount;
    }

    private Map<String, Long> queryGroupByMetrics(String condition, long startTime, long endTime, int limit) {
        try {
            GenericServiceAPIResponseEntity response = client.search()
                .pageSize(Integer.MAX_VALUE)
                .query(condition)
                .startTime(startTime)
                .endTime(endTime).send();
            if (!response.isSuccess()) {
                LOG.error(response.getException());
                return null;
            }
            List<Map<List<String>, List<Double>>> result = response.getObj();
            return parseQueryResult(result, limit);
        } catch (EagleServiceClientException e) {
            LOG.error(e.getMessage(), e);
            return new HashMap<>();
        }
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(initialDelayMin, periodInMin, TimeUnit.MINUTES);
    }

    public static class JobSummaryInfo {
        public String key;
        public long numOfJobs;
        public double ratio;

        public String getKey() {
            return key;
        }

        public long getNumOfJobs() {
            return numOfJobs;
        }

        public double getRatio() {
            return ratio;
        }
    }
}
