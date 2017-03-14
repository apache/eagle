/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package org.apache.eagle.jpm.util.resourcefetch;

import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourcefetch.connection.InputStreamUtils;
import org.apache.eagle.jpm.util.resourcefetch.ha.HAURLSelector;
import org.apache.eagle.jpm.util.resourcefetch.ha.HAURLSelectorImpl;
import org.apache.eagle.jpm.util.resourcefetch.model.AppInfo;
import org.apache.eagle.jpm.util.resourcefetch.model.AppsWrapper;
import org.apache.eagle.jpm.util.resourcefetch.model.ClusterInfo;
import org.apache.eagle.jpm.util.resourcefetch.model.ClusterInfoWrapper;
import org.apache.eagle.jpm.util.resourcefetch.url.*;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RMResourceFetcher implements ResourceFetcher<AppInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(RMResourceFetcher.class);
    private final HAURLSelector selector;
    //private final ServiceURLBuilder jobListServiceURLBuilder;
    private final ServiceURLBuilder sparkCompleteJobServiceURLBuilder;
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    static {
        OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    public RMResourceFetcher(String[] rmBasePaths) {
        //this.jobListServiceURLBuilder = new JobListServiceURLBuilderImpl();
        this.sparkCompleteJobServiceURLBuilder = new SparkCompleteJobServiceURLBuilderImpl();
        this.selector = new HAURLSelectorImpl(rmBasePaths, new RmActiveTestURLBuilderImpl(), Constants.CompressionType.NONE);
    }

    public HAURLSelector getSelector() {
        return selector;
    }

    private List<AppInfo> doFetchApplicationsList(String urlString, Constants.CompressionType compressionType) {
        List<AppInfo> result = new ArrayList<>();
        InputStream is = null;
        try {
            LOG.info("Going to query cluster applications list: " + urlString);
            is = InputStreamUtils.getInputStream(urlString, null, compressionType);
            final AppsWrapper appWrapper = OBJ_MAPPER.readValue(is, AppsWrapper.class);
            if (appWrapper != null && appWrapper.getApps() != null
                && appWrapper.getApps().getApp() != null) {
                result = appWrapper.getApps().getApp();
            }
            LOG.info("Successfully fetched {} AppInfos from url {}", result.size(), urlString);
        } catch (Exception e) {
            LOG.error("Fail to query {} due to {}", urlString, e.getMessage());
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (Exception e) {
                    LOG.warn("{}", e);
                }
            }
        }
        return result;
    }

    public String getRunningJobURL(Constants.JobType jobType, String startTime, String endTime, String limit) {
        String condition = "";
        limit = ((limit == null || limit.isEmpty()) ? "" : "&limit=" + limit);
        if (startTime == null && endTime == null) {
            condition = String.format("applicationTypes=%s%s&", jobType, limit);
        } else if (startTime == null) {
            condition = String.format("applicationTypes=%s&startedTimeEnd=%s%s&", jobType, endTime, limit);
        } else if (endTime == null) {
            condition = String.format("applicationTypes=%s&startedTimeBegin=%s%s&", jobType, startTime, limit);
        } else {
            condition = String.format("applicationTypes=%s&startedTimeBegin=%s&startedTimeEnd=%s%s&",
                    jobType, startTime, endTime, limit);
        }
        String url = URLUtil.removeTrailingSlash(selector.getSelectedUrl());
        return String.format("%s/%s?%sstate=RUNNING&%s", url, Constants.V2_APPS_URL, condition,
                Constants.ANONYMOUS_PARAMETER);
    }

    private String getMRFinishedJobURL(String lastFinishedTime) {
        String url = URLUtil.removeTrailingSlash(selector.getSelectedUrl());
        return url + "/" + Constants.V2_APPS_URL
                + "?applicationTypes=MAPREDUCE&state=FINISHED&finishedTimeBegin="
                + lastFinishedTime + "&" + Constants.ANONYMOUS_PARAMETER;
    }

    private String getAcceptedAppURL() {
        String baseUrl = URLUtil.removeTrailingSlash(selector.getSelectedUrl());
        return String.format("%s/%s?state=ACCEPTED&%s", baseUrl, Constants.V2_APPS_URL, Constants.ANONYMOUS_PARAMETER);
    }

    private List<AppInfo> doFetchRunningApplicationsList(Constants.JobType jobType,
                                                         Constants.CompressionType compressionType,
                                                         Object... parameter) throws Exception {
        Map<String, AppInfo> result = new HashMap();
        List<AppInfo> apps = new ArrayList<>();
        try {
            selector.checkUrl();

            String limit = "";
            int requests = 1;
            int requestTimeInHour = 6;

            switch (parameter.length) {
                case 0 :
                    String urlString = getRunningJobURL(jobType, null, null, null);
                    return doFetchApplicationsList(urlString, compressionType);
                case 1 :
                    limit = String.valueOf(parameter[0]);
                    break;
                case 2 :
                    limit = String.valueOf(parameter[0]);
                    requests = (int) parameter[1];
                    break;
                case 3 :
                    limit = String.valueOf(parameter[0]);
                    requests = (int) parameter[1];
                    requestTimeInHour = (int) parameter[2];
                    break;
                default :
                    throw new InvalidParameterException("parameter list: limit, requests, requestTimeRange");
            }

            if (requests <= 1) {
                String urlString = getRunningJobURL(jobType, null, null, limit);
                return doFetchApplicationsList(urlString, compressionType);
            }

            long requestTimeSpan = requestTimeInHour * DateTimeUtil.ONEHOUR;
            long interval =  requestTimeSpan / (requests - 1);
            long currentTime = System.currentTimeMillis();
            long earliestTime = currentTime - requestTimeSpan;
            List<String> requestUrls = new ArrayList<>();

            requestUrls.add(getRunningJobURL(jobType, null, String.valueOf(earliestTime), limit));

            long start = earliestTime;
            long end = earliestTime + interval;
            for (; end < currentTime; start = end, end += interval) {
                requestUrls.add(getRunningJobURL(jobType, String.valueOf(start), String.valueOf(end), limit));
            }

            requestUrls.add(getRunningJobURL(jobType, String.valueOf(start), null, limit));
            LOG.info("{} requests to fetch running MapReduce applications: \n{}", requestUrls.size(),
                    StringUtils.join(requestUrls, "\n"));

            requestUrls.forEach(query ->
                doFetchApplicationsList(query, compressionType).forEach(app -> result.put(app.getId(), app))
            );
        } catch (Exception e) {
            LOG.error("Catch an exception when query url{} : {}", selector.getSelectedUrl(), e.getMessage(), e);
            return apps;
        }
        apps.addAll(result.values());
        return apps;
    }

    private List<AppInfo> doFetchAcceptedApplicationList(Constants.CompressionType compressionType,
                                                         Object... parameter) throws Exception {
        List<AppInfo> apps = new ArrayList<>();
        try {
            selector.checkUrl();
            String url = getAcceptedAppURL();
            return doFetchApplicationsList(url, compressionType);
        } catch (Exception e) {
            LOG.error("Catch an exception when query {} : {}", selector.getSelectedUrl(), e.getMessage(), e);
        }
        return apps;
    }

    private List<AppInfo> getResource(Constants.ResourceType resourceType, Constants.CompressionType compressionType, Object... parameter) throws Exception {
        switch (resourceType) {
            case COMPLETE_SPARK_JOB:
                final String urlString = sparkCompleteJobServiceURLBuilder.build(selector.getSelectedUrl(), (String) parameter[0]);
                return doFetchApplicationsList(urlString, compressionType);
            case RUNNING_SPARK_JOB:
                return doFetchRunningApplicationsList(Constants.JobType.SPARK, compressionType, parameter);
            case RUNNING_MR_JOB:
                return doFetchRunningApplicationsList(Constants.JobType.MAPREDUCE, compressionType, parameter);
            case COMPLETE_MR_JOB:
                return doFetchApplicationsList(getMRFinishedJobURL((String) parameter[0]), compressionType);
            case ACCEPTED_JOB:
                return doFetchAcceptedApplicationList(compressionType, parameter);
            default:
                throw new Exception("Not support resourceType :" + resourceType);
        }
    }

    public List<AppInfo> getResource(Constants.ResourceType resourceType, Object... parameter) throws Exception {
        try {
            return getResource(resourceType, Constants.CompressionType.GZIP, parameter);
        } catch (java.util.zip.ZipException ex) {
            return getResource(resourceType, Constants.CompressionType.NONE, parameter);
        }
    }

    private String getClusterInfoURL() {
        return selector.getSelectedUrl() + "/" + Constants.YARN_API_CLUSTER_INFO + "?" + Constants.ANONYMOUS_PARAMETER;
    }

    public ClusterInfo getClusterInfo() throws Exception {
        InputStream is = null;
        try {
            selector.checkUrl();
            final String urlString = getClusterInfoURL();
            LOG.info("Calling yarn api to fetch cluster info: " + urlString);
            is = InputStreamUtils.getInputStream(urlString, null, Constants.CompressionType.GZIP);
            final ClusterInfoWrapper clusterInfoWrapper = OBJ_MAPPER.readValue(is, ClusterInfoWrapper.class);
            if (clusterInfoWrapper != null && clusterInfoWrapper.getClusterInfo() != null) {
                return clusterInfoWrapper.getClusterInfo();
            }
            return null;
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (Exception e) {
                    LOG.warn("{}", e);
                }
            }
        }
    }
}