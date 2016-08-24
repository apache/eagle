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

import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourcefetch.connection.InputStreamUtils;
import org.apache.eagle.jpm.util.resourcefetch.ha.HAURLSelector;
import org.apache.eagle.jpm.util.resourcefetch.ha.HAURLSelectorImpl;
import org.apache.eagle.jpm.util.resourcefetch.model.AppInfo;
import org.apache.eagle.jpm.util.resourcefetch.model.AppsWrapper;
import org.apache.eagle.jpm.util.resourcefetch.model.ClusterInfo;
import org.apache.eagle.jpm.util.resourcefetch.model.ClusterInfoWrapper;
import org.apache.eagle.jpm.util.resourcefetch.url.JobListServiceURLBuilderImpl;
import org.apache.eagle.jpm.util.resourcefetch.url.ServiceURLBuilder;
import org.apache.eagle.jpm.util.resourcefetch.url.SparkCompleteJobServiceURLBuilderImpl;
import org.apache.eagle.jpm.util.resourcefetch.url.URLUtil;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class RMResourceFetcher implements ResourceFetcher<AppInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(RMResourceFetcher.class);
    private final HAURLSelector selector;
    private final ServiceURLBuilder jobListServiceURLBuilder;
    private final ServiceURLBuilder sparkCompleteJobServiceURLBuilder;
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    static {
        OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    public RMResourceFetcher(String[] rmBasePaths) {
        this.jobListServiceURLBuilder = new JobListServiceURLBuilderImpl();
        this.sparkCompleteJobServiceURLBuilder = new SparkCompleteJobServiceURLBuilderImpl();

        this.selector = new HAURLSelectorImpl(rmBasePaths, jobListServiceURLBuilder, Constants.CompressionType.GZIP);
    }

    private void checkUrl() throws IOException {
        if (!selector.checkUrl(jobListServiceURLBuilder.build(selector.getSelectedUrl(), Constants.JobState.RUNNING.name()))) {
            selector.reSelectUrl();
        }
    }

    private List<AppInfo> doFetchFinishApplicationsList(String urlString, Constants.CompressionType compressionType) throws Exception {
        List<AppInfo> result;
        InputStream is = null;
        try {
            checkUrl();
            LOG.info("Going to call yarn api to fetch finished application list: " + urlString);
            is = InputStreamUtils.getInputStream(urlString, null, compressionType);
            final AppsWrapper appWrapper = OBJ_MAPPER.readValue(is, AppsWrapper.class);
            if (appWrapper != null && appWrapper.getApps() != null
                && appWrapper.getApps().getApp() != null) {
                result = appWrapper.getApps().getApp();
                return result;
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

    private String getSparkRunningJobURL() {
        StringBuilder sb = new StringBuilder();
        sb.append(selector.getSelectedUrl()).append("/").append(Constants.V2_APPS_URL);
        sb.append("?applicationTypes=SPARK&state=RUNNING&");
        sb.append(Constants.ANONYMOUS_PARAMETER);
        return sb.toString();
    }

    private String getMRRunningJobURL() {
        return String.format("%s/%s?applicationTypes=MAPREDUCE&state=RUNNING&%s",
            selector.getSelectedUrl(),
            Constants.V2_APPS_URL,
            Constants.ANONYMOUS_PARAMETER);
    }

    public String getMRFinishedJobURL(String lastFinishedTime) {
        String url = URLUtil.removeTrailingSlash(selector.getSelectedUrl());
        StringBuilder sb = new StringBuilder();
        sb.append(url).append("/").append(Constants.V2_APPS_URL);
        sb.append("?applicationTypes=MAPREDUCE&state=FINISHED&finishedTimeBegin=");
        sb.append(lastFinishedTime).append("&").append(Constants.ANONYMOUS_PARAMETER);

        return sb.toString();
    }

    private List<AppInfo> doFetchRunningApplicationsList(String urlString, Constants.CompressionType compressionType) throws Exception {
        List<AppInfo> result;
        InputStream is = null;
        try {
            checkUrl();
            LOG.info("Going to call yarn api to fetch running application list: " + urlString);
            is = InputStreamUtils.getInputStream(urlString, null, compressionType);
            final AppsWrapper appWrapper = OBJ_MAPPER.readValue(is, AppsWrapper.class);
            if (appWrapper != null && appWrapper.getApps() != null && appWrapper.getApps().getApp() != null) {
                result = appWrapper.getApps().getApp();
                return result;
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

    private List<AppInfo> getResource(Constants.ResourceType resoureType, Constants.CompressionType compressionType, Object... parameter) throws Exception {
        switch (resoureType) {
            case COMPLETE_SPARK_JOB:
                final String urlString = sparkCompleteJobServiceURLBuilder.build((String) parameter[0]);
                return doFetchFinishApplicationsList(urlString, compressionType);
            case RUNNING_SPARK_JOB:
                return doFetchRunningApplicationsList(getSparkRunningJobURL(), compressionType);
            case RUNNING_MR_JOB:
                return doFetchRunningApplicationsList(getMRRunningJobURL(), compressionType);
            case COMPLETE_MR_JOB:
                return doFetchFinishApplicationsList(getMRFinishedJobURL((String) parameter[0]), compressionType);
            default:
                throw new Exception("Not support resourceType :" + resoureType);
        }
    }

    public List<AppInfo> getResource(Constants.ResourceType resoureType, Object... parameter) throws Exception {
        try {
            return getResource(resoureType, Constants.CompressionType.GZIP, parameter);
        } catch (java.util.zip.ZipException ex) {
            return getResource(resoureType, Constants.CompressionType.NONE, parameter);
        }
    }

    private String getClusterInfoURL() {
        StringBuilder sb = new StringBuilder();
        sb.append(selector.getSelectedUrl()).append("/").append(Constants.YARN_API_CLUSTER_INFO).append("?" + Constants.ANONYMOUS_PARAMETER);
        return sb.toString();
    }

    public ClusterInfo getClusterInfo() throws Exception {
        InputStream is = null;
        try {
            checkUrl();
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
