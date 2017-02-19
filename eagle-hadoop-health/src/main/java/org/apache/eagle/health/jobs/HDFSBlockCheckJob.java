/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.health.jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.app.job.MonitorJob;
import org.apache.eagle.app.job.MonitorResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;

/**
 * 1. Load latest block snapshot from eagle store
 * 2. Load Corrupt Block from JMX instead of FSCK for lighter pressure on Name Node
 * 3. Compare corrupt block and history snapshot about newly added corrupt block and resumed block,
 * and try to provide as much information as possible for diagnose about corrupt and resumed blocks.
 */
public class HDFSBlockCheckJob extends MonitorJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSBlockCheckJob.class);
    private static final String HDFS_PROPS_KEY = "connectionProps";

    private Configuration configuration;
    private UserGroupInformation ugi;
    private URLConnectionFactory connectionFactory;
    private boolean isSpnegoEnabled;
    private String siteId;
    private FileSystem fileSystem;
    private URI hdfsInfoServerUrl;

    @Override
    protected void prepare(JobExecutionContext context) throws JobExecutionException {
        Preconditions.checkArgument(context.getMergedJobDataMap().containsKey(HDFS_PROPS_KEY), HDFS_PROPS_KEY);

        try {
            // Previous dump under-replication blocks
            Properties properties = new Properties();
            String hdfsProps = context.getMergedJobDataMap().getString(HDFS_PROPS_KEY);
            if (StringUtils.isNotBlank(hdfsProps)) {
                properties.load(new StringReader(hdfsProps));
            }
            configuration = new Configuration();
            configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                String propName = (String) entry.getKey();
                String propValue = (String) entry.getValue();
                configuration.set(propName, propValue);
            }
            this.ugi = UserGroupInformation.getCurrentUser();
            this.connectionFactory = URLConnectionFactory.newDefaultURLConnectionFactory(configuration);
            this.isSpnegoEnabled = UserGroupInformation.isSecurityEnabled();
            this.siteId = "sandbox";
            this.fileSystem = FileSystem.get(this.configuration);
            this.hdfsInfoServerUrl = DFSUtil.getInfoServer(HAUtil.getAddressOfActive(this.fileSystem), this.configuration, DFSUtil.getHttpClientScheme(this.configuration));

        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw new JobExecutionException(e);
        }
    }

    @Override
    protected MonitorResult execute() throws JobExecutionException, Exception {
        final StringBuilder url = new StringBuilder();
        url.append("/jmx?ugi=")
            .append(ugi.getShortUserName())
            .append("&qry=Hadoop:service=NameNode,name=NameNodeInfo");
        url.insert(0, hdfsInfoServerUrl.toString());
        LOGGER.info("Connecting to namenode via " + url.toString());

        URLConnection connection;
        try {
            connection = connectionFactory.openConnection(new URL(url.toString()), isSpnegoEnabled);
        } catch (AuthenticationException e) {
            throw new IOException(e);
        }
        InputStream stream = connection.getInputStream();
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String,Object> beansMap = objectMapper.readValue(stream, TypeFactory.defaultInstance().constructMapType(HashMap.class, String.class, Object.class));
        LinkedHashMap metricMap = ((ArrayList<LinkedHashMap>) beansMap.get("beans")).get(0);
        onHadopNameNodeInfoMetric(metricMap);
        return MonitorResult.ok("OK");
    }

    private void onHadopNameNodeInfoMetric(LinkedHashMap metricMap) {
        String blockPoolId = (String) metricMap.get("BlockPoolId");
        int numberOfMissingBlocks = (int) metricMap.get("NumberOfMissingBlocks");
        int NumberOfMissingBlocksWithReplicationFactorOne = (int) metricMap.get("NumberOfMissingBlocksWithReplicationFactorOne");
        String corruptFilesStr = (String) metricMap.get("CorruptFiles");
        String liveNodesStr = (String) metricMap.get("LiveNodes");
        String deadNodesStr = (String) metricMap.get("DeadNodes");
        String decomNodesStr = (String) metricMap.get("DecomNodes");
        LOGGER.info(metricMap.toString());
    }

    @Override
    protected void close() throws JobExecutionException {
        if (this.fileSystem != null) {
            try {
                this.fileSystem.close();
            } catch (IOException e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
    }
}