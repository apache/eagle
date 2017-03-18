/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.jpm.mr.running.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.jpm.mr.running.MRRunningJobConfig;
import org.apache.eagle.jpm.mr.runningentity.AppStreamInfo;
import org.apache.eagle.jpm.mr.runningentity.YarnAppAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourcefetch.RMResourceFetcher;
import org.apache.eagle.jpm.util.resourcefetch.model.AppInfo;
import org.apache.eagle.jpm.util.resourcefetch.url.URLUtil;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.eagle.jpm.mr.runningentity.AppStreamInfo.convertAppToStream;

public class MRRunningAppMetricBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MRRunningAppMetricBolt.class);

    private MRRunningJobConfig config;
    private IEagleServiceClient client;
    private RMResourceFetcher fetcher;
    private OutputCollector collector;
    private String site;

    private static final long AGGREGATE_INTERVAL = DateTimeUtil.ONEMINUTE;

    private static final String USER_TAG = "user";
    private static final String QUEUE_TAG = "queue";
    private static final String SITE_TAG = "site";

    @SuppressWarnings("serial")
    public static HashMap<String, String> metrics = new HashMap<String, String>() {
        {
            put(Constants.MetricName.HADOOP_APPS_ALLOCATED_MB, "getAllocatedMB");
            put(Constants.MetricName.HADOOP_APPS_ALLOCATED_VCORES, "getAllocatedVCores");
            put(Constants.MetricName.HADOOP_APPS_RUNNING_CONTAINERS, "getRunningContainers");
        }
    };

    public MRRunningAppMetricBolt(MRRunningJobConfig config) {
        this.config = config;
        this.site = config.getConfig().getString("siteId");
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.client = new EagleServiceClientImpl(config.getConfig());
        this.fetcher = new RMResourceFetcher(config.getEndpointConfig().rmUrls);
    }

    @Override
    public void execute(Tuple input) {
        List<AppInfo> runningApps = (List<AppInfo>) input.getValue(0);
        if (runningApps == null || runningApps.isEmpty()) {
            LOG.warn("App list is empty");
        }
        try {
            Map<String, GenericMetricEntity> appMetrics = parseRunningAppMetrics(runningApps);
            List<YarnAppAPIEntity> acceptedApps = parseAcceptedApp();
            flush(appMetrics, acceptedApps);
        } catch (Exception e) {
            LOG.error("Fetal error is caught {}", e.getMessage(), e);
        }
    }

    private void createMetric(Map<String, GenericMetricEntity> appMetricEntities,
                              long timestamp, Map<String, String> tags, String metricName, int value) {
        String key = metricName + tags.toString() + " " + timestamp;
        GenericMetricEntity entity = appMetricEntities.get(key);
        if (entity == null) {
            entity = new GenericMetricEntity();
            entity.setTags(tags);
            entity.setTimestamp(timestamp);
            entity.setPrefix(metricName);
            entity.setValue(new double[] {0.0});
            appMetricEntities.put(key, entity);
        }
        double lastValue = entity.getValue()[0];
        entity.setValue(new double[] {lastValue + value});
    }

    private Map<String, String> generateMetricTags(AggLevel level, AppInfo app) {
        Map<String, String> tags = new HashMap<>();
        tags.put(SITE_TAG, site);
        switch (level) {
            case CLUSTER : break;
            case QUEUE :
                tags.put(QUEUE_TAG, app.getQueue());
                break;
            case USER :
                tags.put(USER_TAG, app.getUser());
                break;
            default :
                LOG.warn("Unsupported Aggregation Level {}", level);
        }
        return tags;
    }

    public Map<String, GenericMetricEntity> parseRunningAppMetrics(List<AppInfo> runningApps) throws Exception {
        long timestamp = System.currentTimeMillis() / AGGREGATE_INTERVAL * AGGREGATE_INTERVAL;
        Map<String, GenericMetricEntity> appMetricEntities = new HashMap<>();
        for (AppInfo app : runningApps) {
            for (AggLevel level : AggLevel.values()) {
                Map<String, String> tags = generateMetricTags(level, app);
                for (java.util.Map.Entry<String, String> entry : metrics.entrySet()) {
                    Method method = AppInfo.class.getMethod(entry.getValue());
                    Integer value = (Integer) method.invoke(app);
                    String metricName = String.format(entry.getKey(), level.name);
                    createMetric(appMetricEntities, timestamp, tags, metricName, value);
                }
            }
        }
        return appMetricEntities;
    }

    public List<YarnAppAPIEntity> parseAcceptedApp() {
        List<YarnAppAPIEntity> acceptedApps = new ArrayList<>();
        try {
            List<AppInfo> apps = fetcher.getResource(Constants.ResourceType.ACCEPTED_JOB,
                    config.getEndpointConfig().limitPerRequest);

            if (apps != null) {
                LOG.info("successfully fetch {} accepted jobs from {}", apps.size(), fetcher.getSelector().getSelectedUrl());
                for (AppInfo app : apps) {
                    Map<String, String> tags = new HashMap<>();
                    tags.put(AppStreamInfo.SITE, config.getConfig().getString("siteId"));
                    tags.put(AppStreamInfo.ID, app.getId());
                    tags.put(AppStreamInfo.QUEUE, app.getQueue());
                    tags.put(AppStreamInfo.USER, app.getUser());

                    YarnAppAPIEntity appAPIEntity = new YarnAppAPIEntity();
                    appAPIEntity.setTags(tags);
                    appAPIEntity.setTrackingUrl(buildAcceptedAppTrackingURL(app.getId()));
                    appAPIEntity.setAppName(app.getName());
                    appAPIEntity.setClusterUsagePercentage(app.getClusterUsagePercentage());
                    appAPIEntity.setQueueUsagePercentage(app.getQueueUsagePercentage());
                    appAPIEntity.setElapsedTime(app.getElapsedTime());
                    appAPIEntity.setStartedTime(app.getStartedTime());
                    appAPIEntity.setState(app.getState());
                    appAPIEntity.setTimestamp(app.getStartedTime());
                    acceptedApps.add(appAPIEntity);
                    collector.emit(new Values("", convertAppToStream(appAPIEntity)));
                }
            }
        } catch (Exception e) {
            LOG.error("fetch accepted apps failed {}", e.getMessage(), e);
        }
        return acceptedApps;
    }

    private String buildAcceptedAppTrackingURL(String appId) {
        String url = URLUtil.removeTrailingSlash(fetcher.getSelector().getSelectedUrl());
        return String.format("%s/%s/%s?%s", url, Constants.V2_APPS_URL, appId, Constants.ANONYMOUS_PARAMETER);
    }

    private void flush(Map<String, GenericMetricEntity> appMetrics, List<YarnAppAPIEntity> acceptedApps) {
        List<TaggedLogAPIEntity> entities = new ArrayList<>();
        if (appMetrics != null && !appMetrics.isEmpty()) {
            LOG.info("crawled {} running app metrics", appMetrics.size());
            entities.addAll(appMetrics.values());
        }
        if (acceptedApps != null && !acceptedApps.isEmpty()) {
            LOG.info("crawled {} accepted apps", acceptedApps.size());
            //entities.addAll(acceptedApps);
        }
        try {
            client.create(entities);
            LOG.info("Successfully create {} metrics", entities.size());
        } catch (Exception e) {
            LOG.error("Fail to create {} metrics due to {}", entities.size(), e.getMessage(), e);
        }
    }

    private enum AggLevel {
        CLUSTER("cluster"), QUEUE("queue"), USER("user");

        private String name;

        AggLevel(String name) {
            this.name = name;
        }
    }

    @Override
    public void cleanup() {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("f1", "message"));
    }
}
