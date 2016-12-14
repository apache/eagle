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
import org.apache.eagle.app.resource.ApplicationResource;
import org.apache.eagle.app.service.ApplicationEmailService;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.metadata.model.ApplicationEntity;
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
    private static final String SERVICE_PATH = "application.dailyJobReport";
    private static final String DAILY_SENT_HOUROFDAY = "application.dailyJobReport.reportHourTime";
    private static final String DAILY_SENT_PERIOD = "application.dailyJobReport.reportPeriodInHour";
    private static final String NUM_TOP_USERS  = "application.dailyJobReport.numTopUsers";
    private static final String JOB_OVERTIME_LIMIT_HOUR  = "application.dailyJobReport.jobOvertimeLimitInHour";

    private Config config;
    private IEagleServiceClient client;
    @Inject private ApplicationResource applicationResource;
    private ApplicationEmailService emailService;
    private boolean isDailySent = false;
    private long lastSentTime;

    private int dailySentHour;
    private int dailySentPeriod;
    private int numTopUsers = 10;
    private int jobOvertimeLimit = 6;

    // scheduler
    private  int initialDelayMin = 10;
    private  int periodInMin = 60;
    private TimeZone timeZone;

    @Inject
    public MRHistoryJobDailyReporter(Config config) {
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
    }

    private boolean isSentHour(int currentHour) {
        return Math.abs(currentHour - dailySentHour) % dailySentPeriod == 0;
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
            calendar.setTimeInMillis(lastSentTime);
            LOG.info("last sent time = {} %s", calendar.getTime(), timeZone.getID());
            try {
                Collection<String> sites = loadSiteList("MR_HISTORY_JOB_APP");
                if (sites == null && sites.isEmpty()) {
                    LOG.warn("application MR_HISTORY_JOB_APP does not run on any sites!");
                    return;
                }
                for (String site : sites) {
                    long reportTime = (currentTimestamp / DateTimeUtil.ONEHOUR + currentHour / dailySentPeriod) * DateTimeUtil.ONEHOUR;
                    sendByEmail(buildAlertData(site, reportTime));
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

    private Collection<String> loadSiteList(String appType) {
        Set<String> sites = new HashSet<>();
        Collection<ApplicationEntity> apps = applicationResource.getApplicationEntities(null).getData();
        for (ApplicationEntity app : apps) {
            if (app.getDescriptor().getType().equalsIgnoreCase(appType) && app.getStatus().equals(ApplicationEntity.Status.RUNNING)) {
                sites.add(app.getSite().getSiteId());
            }
        }
        return sites;
    }

    private Map<String, Object> buildAlertData(String site, long endTime) {
        Map<String, Object> data = new HashMap<>();
        this.client = new EagleServiceClientImpl(config);
        long startTime = endTime - DateTimeUtil.ONEHOUR * dailySentPeriod;
        LOG.info("Going to report job summery info for site {} from {} to {}", site,
            DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime),
            DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime));
        try {
            data.putAll(buildJobSummery(site, startTime, endTime));
            data.putAll(buildFailedJobInfo(site, startTime, endTime));
            data.putAll(buildSucceededJobInfo(site, startTime, endTime));
            data.putAll(buildFinishedJobInfo(site, startTime, endTime));
            data.put("numTopUsers", numTopUsers);
            data.put("jobOvertimeLimit", jobOvertimeLimit);
            data.put("alertTitle", String.format("%s Daily Job Report", site.toUpperCase()));
            data.put("reportRange", String.format("%s ~ %s %s",
                DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime),
                DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime),
                DateTimeUtil.CURRENT_TIME_ZONE.getID()));
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                LOG.info("fail to close eagle service client");
            }
        }
        return data;
    }

    private Map<String, Double> parseQueryResult(GenericServiceAPIResponseEntity<Map<List<String>, List<Double>>> responseEntity, int limit) {
        Map<String, Double> stateCount = new TreeMap<>();
        if (responseEntity.isSuccess()) {
            responseEntity.getObj().stream().forEach(map -> {
                for (Map.Entry<List<String>, List<Double>> entry : map.entrySet()) {
                    if (stateCount.size() >= limit) {
                        break;
                    }
                    stateCount.put(entry.getKey().get(0), entry.getValue().get(0));
                }
            });
        }
        return stateCount;
    }

    private Map<String, Double> queryGroupByMetrics(String condition, long startTime, long endTime, int limit) {
        try {
            GenericServiceAPIResponseEntity<Map<List<String>, List<Double>>> response = client.search()
                .pageSize(Integer.MAX_VALUE)
                .query(condition)
                .startTime(startTime)
                .endTime(endTime).send();
            Map<String, Double> userCount = parseQueryResult(response, limit);
            return userCount;
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
        Map<String, Double> jobSummery = queryGroupByMetrics(query, startTime, endTime, Integer.MAX_VALUE);
        if (jobSummery == null || jobSummery.isEmpty()) {
            return data;
        }
        Optional<Double> totalJobs = jobSummery.values().stream().reduce((a, b) -> a + b);
        for (Map.Entry<String, Double> entry : jobSummery.entrySet()) {
            JobSummeryInfo summeryInfo = new JobSummeryInfo();
            summeryInfo.status = entry.getKey();
            summeryInfo.numOfJobs = entry.getValue();
            summeryInfo.ratio = entry.getValue() / totalJobs.get();
            statusCount.add(summeryInfo);
        }
        data.put("summeryInfo", statusCount);
        return data;
    }

    private Map<String, Object> buildFailedJobInfo(String site, long startTime, long endTime) {
        Map<String, Object> data = new HashMap<>();
        String query = String.format("%s[@site=\"%s\" and @currentState=\"FAILED\" and @endTime<=%s]<@user>{count}.{count desc}",
            Constants.JPA_JOB_EXECUTION_SERVICE_NAME, site, endTime);

        Map<String, Double> failedJobUsers = queryGroupByMetrics(query, startTime, endTime, numTopUsers);
        if (failedJobUsers == null || failedJobUsers.isEmpty()) {
            LOG.warn("Result set is empty for query={}", query);
            return data;
        }
        data.put("failedJobUsers", failedJobUsers);
        data.put("joblink", String.format("http://%s:%d/#/site/%s/jpm/list?startTime=%s&endTime=%s",
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
        Map<String, Double> succeededJobUsers = queryGroupByMetrics(query, startTime, endTime, numTopUsers);
        if (succeededJobUsers == null || succeededJobUsers.isEmpty()) {
            LOG.warn("Result set is empty for query={}", query);
            return data;
        }
        data.put("succeededJobUsers", succeededJobUsers);
        return data;
    }

    private Map<String, Object> buildFinishedJobInfo(String site, long startTime, long endTime) {
        Map<String, Object> data = new HashMap<>();
        String query = String.format("%s[@site=\"%s\" and @endTime<=%s]<@user>{count}.{count desc}",
            Constants.JPA_JOB_EXECUTION_SERVICE_NAME, site, endTime);
        Map<String, Double> jobUsers = queryGroupByMetrics(query, startTime, endTime, numTopUsers);
        if (jobUsers == null || jobUsers.isEmpty()) {
            LOG.warn("Result set is empty for query={}", query);
            return data;
        }
        data.put("finishedJobUsers", jobUsers);
        return data;
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(initialDelayMin, periodInMin, TimeUnit.MINUTES);
    }

    public static class JobSummeryInfo {
        public String status;
        public double numOfJobs;
        public double ratio;

        public String getStatus() {
            return status;
        }

        public double getNumOfJobs() {
            return numOfJobs;
        }

        public double getRatio() {
            return ratio;
        }
    }
}
