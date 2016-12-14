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
    private static final String TIMEZONE_PATH = "service.timezone";
    private static final String DAILY_SENT_HOUROFDAY = "application.dailyJobReport.reportHourTime";
    private static final String DAILY_SENT_PERIOD = "application.dailyJobReport.reportPeriodInHour";
    private static final String NUM_TOP_USERS  = "application.dailyJobReport.numTopUsers";
    private static final String JOB_OVERTIME_LIMIT_HOUR  = "application.dailyJobReport.jobOvertimeLimitInHour";

    public static final String SERVICE_PATH = "application.dailyJobReport";
    public static final String APP_TYPE = "MR_HISTORY_JOB_APP";

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
        this.emailService = new ApplicationEmailService(config, SERVICE_PATH);
        this.dailySentHour = config.getInt(DAILY_SENT_HOUROFDAY);
        this.dailySentPeriod = config.getInt(DAILY_SENT_PERIOD);

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
                    String subject = String.format("%s %s", site.toUpperCase(), config.getString(SERVICE_PATH + "." + AlertEmailConstants.SUBJECT));
                    Map<String, Object> alertData = buildAlertData(site, calendar.getTimeInMillis());
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

    private Map<String, Object> buildAlertData(String site, long endTime) {
        StopWatch watch = new StopWatch();
        Map<String, Object> data = new HashMap<>();
        this.client = new EagleServiceClientImpl(config);
        long startTime = endTime - DateTimeUtil.ONEHOUR * dailySentPeriod;
        LOG.info("Going to report job summery info for site {} from {} to {}", site,
            DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime),
            DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime));
        try {
            watch.start();
            data.putAll(buildJobSummery(site, startTime, endTime));
            data.putAll(buildFailedJobInfo(site, startTime, endTime));
            data.putAll(buildSucceededJobInfo(site, startTime, endTime));
            data.putAll(buildFinishedJobInfo(site, startTime, endTime));
            data.put(NUM_TOP_USERS_KEY, numTopUsers);
            data.put(JOB_OVERTIME_LIMIT_KEY, jobOvertimeLimit);
            data.put(ALERT_TITLE_KEY, String.format("%s Daily Job Report", site.toUpperCase()));
            data.put(REPORT_RANGE_KEY, String.format("%s ~ %s %s",
                DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime),
                DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime),
                DateTimeUtil.CURRENT_TIME_ZONE.getID()));
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
                return null;
            }
            List<Map<List<String>, List<Double>>> result = response.getObj();
            return parseQueryResult(result, limit);
        } catch (EagleServiceClientException e) {
            LOG.error(e.getMessage(), e);
            return new HashMap<>();
        }
    }

    private Map<String, Object> buildJobSummery(String site, long startTime, long endTime) {
        Map<String, Object> data = new HashMap<>();
        List<JobSummeryInfo> statusCount = new ArrayList<>();
        String query = String.format("%s[@site=\"%s\" and @endTime<=%s]<@currentState>{count}",
            Constants.JPA_JOB_EXECUTION_SERVICE_NAME, site, endTime);
        Map<String, Long> jobSummery = queryGroupByMetrics(query, startTime, endTime, Integer.MAX_VALUE);
        if (jobSummery == null || jobSummery.isEmpty()) {
            return data;
        }
        Optional<Long> totalJobs = jobSummery.values().stream().reduce((a, b) -> a + b);
        for (Map.Entry<String, Long> entry : jobSummery.entrySet()) {
            JobSummeryInfo summeryInfo = new JobSummeryInfo();
            summeryInfo.status = entry.getKey();
            summeryInfo.numOfJobs = entry.getValue();
            summeryInfo.ratio = String.format("%.2f", entry.getValue() * 1d / totalJobs.get());
            statusCount.add(summeryInfo);
        }
        data.put(SUMMARY_INFO_KEY, statusCount);
        return data;
    }

    private Map<String, Object> buildFailedJobInfo(String site, long startTime, long endTime) {
        Map<String, Object> data = new HashMap<>();
        String query = String.format("%s[@site=\"%s\" and @currentState=\"FAILED\" and @endTime<=%s]<@user>{count}.{count desc}",
            Constants.JPA_JOB_EXECUTION_SERVICE_NAME, site, endTime);

        Map<String, Long> failedJobUsers = queryGroupByMetrics(query, startTime, endTime, numTopUsers);
        if (failedJobUsers == null || failedJobUsers.isEmpty()) {
            LOG.warn("Result set is empty for query={}", query);
            return data;
        }
        data.put(FAILED_JOB_USERS_KEY, failedJobUsers);
        data.put(EAGLE_JOB_LINK_KEY, String.format("http://%s:%d/#/site/%s/jpm/list?startTime=%s&endTime=%s",
            config.getString(SERVICE_HOST),
            config.getInt(SERVICE_PORT),
            site,
            DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime),
            DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime)));
        return data;
    }

    private Map<String, Object> buildSucceededJobInfo(String site, long startTime, long endTime) {
        Map<String, Object> data = new HashMap<>();
        long overtimeLimit = jobOvertimeLimit * DateTimeUtil.ONEHOUR;
        String query = String.format("%s[@site=\"%s\" and @currentState=\"SUCCEEDED\" and @durationTime>%s and @endTime<=%s]<@user>{count}.{count desc}",
            Constants.JPA_JOB_EXECUTION_SERVICE_NAME, site, overtimeLimit, endTime);
        Map<String, Long> succeededJobUsers = queryGroupByMetrics(query, startTime, endTime, numTopUsers);
        if (succeededJobUsers == null || succeededJobUsers.isEmpty()) {
            LOG.warn("Result set is empty for query={}", query);
            return data;
        }
        data.put(SUCCEEDED_JOB_USERS_KEY, succeededJobUsers);
        return data;
    }

    private Map<String, Object> buildFinishedJobInfo(String site, long startTime, long endTime) {
        Map<String, Object> data = new HashMap<>();
        String query = String.format("%s[@site=\"%s\" and @endTime<=%s]<@user>{count}.{count desc}",
            Constants.JPA_JOB_EXECUTION_SERVICE_NAME, site, endTime);
        Map<String, Long> jobUsers = queryGroupByMetrics(query, startTime, endTime, numTopUsers);
        if (jobUsers == null || jobUsers.isEmpty()) {
            LOG.warn("Result set is empty for query={}", query);
            return data;
        }
        data.put(FINISHED_JOB_USERS_KEY, jobUsers);
        return data;
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(initialDelayMin, periodInMin, TimeUnit.MINUTES);
    }

    public static class JobSummeryInfo {
        public String status;
        public long numOfJobs;
        public String ratio;

        public String getStatus() {
            return status;
        }

        public long getNumOfJobs() {
            return numOfJobs;
        }

        public String getRatio() {
            return ratio;
        }
    }
}
