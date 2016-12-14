/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.service;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.Tuple2StreamMetadata;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.scheme.JsonScheme;
import org.apache.eagle.alert.engine.scheme.JsonStringStreamNameSelector;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.apache.eagle.alert.metric.MetricConfigs;
import org.apache.eagle.alert.utils.AlertConstants;
import org.apache.eagle.app.Application;
import org.apache.eagle.app.environment.ExecutionRuntime;
import org.apache.eagle.app.environment.ExecutionRuntimeManager;
import org.apache.eagle.app.messaging.KafkaStreamSinkConfig;
import org.apache.eagle.app.messaging.KafkaStreamSourceConfig;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.StreamSourceConfig;
import org.apache.eagle.metadata.utils.StreamIdConversions;
import org.apache.eagle.metadata.model.StreamDesc;
import org.apache.eagle.metadata.model.StreamSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Managed Application Action: org.apache.eagle.app.service.ApplicationAction
 * <ul>
 * <li>Application Metadata Entity (Persistence): org.apache.eagle.metadata.model.ApplicationEntity</li>
 * <li>Application Processing Logic (Execution): org.apache.eagle.app.Application</li>
 * <li>Application Lifecycle Listener (Installation): org.apache.eagle.app.ApplicationLifecycle</li>
 * </ul>
 */
public class ApplicationAction implements Serializable {
    private final Config effectiveConfig;
    private final Application application;
    private final ExecutionRuntime runtime;
    private final ApplicationEntity metadata;
    private final IMetadataDao alertMetadataService;
    private static final String APP_METRIC_PREFIX = "eagle.";
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationAction.class);

    /**
     * @param metadata    ApplicationEntity.
     * @param application Application.
     */
    public ApplicationAction(Application application, ApplicationEntity metadata, Config serverConfig, IMetadataDao alertMetadataService) {
        Preconditions.checkNotNull(application, "Application is null");
        Preconditions.checkNotNull(metadata, "ApplicationEntity is null");
        this.application = application;
        this.metadata = metadata;
        this.runtime = ExecutionRuntimeManager.getInstance().getRuntime(application.getEnvironmentType(), serverConfig);
        Map<String, Object> executionConfig = new HashMap<>(metadata.getConfiguration());
        if (executionConfig == null) {
            executionConfig = Collections.emptyMap();
        }
        if (serverConfig.hasPath(MetricConfigs.METRIC_PREFIX_CONF)) {
            LOG.warn("Ignored sever config {} = {}", MetricConfigs.METRIC_PREFIX_CONF, serverConfig.getString(MetricConfigs.METRIC_PREFIX_CONF));
        }

        executionConfig.put("jarPath", metadata.getJarPath());
        executionConfig.put("mode", metadata.getMode().name());
        executionConfig.put(MetricConfigs.METRIC_PREFIX_CONF, APP_METRIC_PREFIX);

        if (serverConfig.hasPath(AlertConstants.COORDINATOR)) {
            this.effectiveConfig = ConfigFactory.parseMap(executionConfig)
                    .withFallback(serverConfig)
                    .withFallback(ConfigFactory.parseMap(metadata.getContext()))
                    .withFallback(serverConfig.getConfig(AlertConstants.COORDINATOR));
        } else {
            this.effectiveConfig = ConfigFactory.parseMap(executionConfig)
                    .withFallback(serverConfig)
                    .withFallback(ConfigFactory.parseMap(metadata.getContext()));
        }
        this.alertMetadataService = alertMetadataService;
    }

    public void doInstall() {
        processStreams();
    }

    private void processStreams() {
        if (metadata.getDescriptor().getStreams() == null) {
            return;
        }

        List<StreamDesc> streamDescToInstall = metadata.getDescriptor().getStreams().stream().map((streamDefinition -> {
            StreamDefinition copied = streamDefinition.copy();
            copied.setSiteId(metadata.getSite().getSiteId());
            copied.setStreamId(StreamIdConversions.formatSiteStreamId(metadata.getSite().getSiteId(), copied.getStreamId()));
            StreamSinkConfig streamSinkConfig = this.runtime.environment()
                    .stream().getSinkConfig(StreamIdConversions.parseStreamTypeId(copied.getSiteId(), copied.getStreamId()), this.effectiveConfig);

            StreamSourceConfig streamSourceConfig = null;

            try {
                streamSourceConfig = this.runtime.environment()
                    .stream().getSourceConfig(StreamIdConversions.parseStreamTypeId(copied.getSiteId(), copied.getStreamId()), this.effectiveConfig);
            } catch (Throwable throwable) {
                // Ignore source config if not set.
            }

            StreamDesc streamDesc = new StreamDesc();
            streamDesc.setSchema(copied);
            streamDesc.setSinkConfig(streamSinkConfig);
            streamDesc.setSourceConfig(streamSourceConfig);
            streamDesc.setStreamId(copied.getStreamId());

            return streamDesc;
        })).collect(Collectors.toList());
        metadata.setStreams(streamDescToInstall);

        // iterate each stream descriptor and create alert datasource for each
        for (StreamDesc streamDesc : streamDescToInstall) {
            // only take care of Kafka sink
            if (streamDesc.getSinkConfig() instanceof KafkaStreamSinkConfig) {
                KafkaStreamSinkConfig kafkaCfg = (KafkaStreamSinkConfig) streamDesc.getSinkConfig();
                Kafka2TupleMetadata datasource = new Kafka2TupleMetadata();
                datasource.setType("KAFKA");
                datasource.setName(metadata.getAppId());
                datasource.setTopic(kafkaCfg.getTopicId());
                datasource.setSchemeCls(JsonScheme.class.getCanonicalName());
                datasource.setProperties(new HashMap<>());

                KafkaStreamSourceConfig streamSourceConfig = (KafkaStreamSourceConfig) streamDesc.getSourceConfig();
                if (streamSourceConfig != null) {
                    Map<String, String> properties = datasource.getProperties();
                    properties.put(AlertConstants.KAFKA_BROKER_ZK_BASE_PATH, streamSourceConfig.getBrokerZkPath());
                    properties.put(AlertConstants.KAFKA_BROKER_ZK_QUORUM, streamSourceConfig.getBrokerZkQuorum());
                }

                Tuple2StreamMetadata tuple2Stream = new Tuple2StreamMetadata();
                Properties prop = new Properties();
                prop.put(JsonStringStreamNameSelector.USER_PROVIDED_STREAM_NAME_PROPERTY, streamDesc.getStreamId());
                tuple2Stream.setStreamNameSelectorProp(prop);
                tuple2Stream.setTimestampColumn("timestamp");
                tuple2Stream.setStreamNameSelectorCls(JsonStringStreamNameSelector.class.getCanonicalName());
                datasource.setCodec(tuple2Stream);
                alertMetadataService.addDataSource(datasource);

                StreamDefinition sd = streamDesc.getSchema();
                sd.setDataSource(metadata.getAppId());
                alertMetadataService.createStream(streamDesc.getSchema());
            }
        }
    }

    public void doUninstall() {
        // we should remove alert data source and stream definition while we do uninstall
        if (metadata.getStreams() == null) {
            return;
        }
        // iterate each stream descriptor and create alert datasource for each
        for (StreamDesc streamDesc : metadata.getStreams()) {
            alertMetadataService.removeDataSource(metadata.getAppId());
            alertMetadataService.removeStream(streamDesc.getStreamId());
        }
    }

    public void doStart() {
        if (metadata.getStreams() == null) {
            processStreams();
        }
        this.runtime.start(this.application, this.effectiveConfig);
    }

    @SuppressWarnings("unchecked")
    public void doStop() {
        this.runtime.stop(this.application, this.effectiveConfig);
    }

    public ApplicationEntity.Status getStatus() {
        return this.runtime.status(this.application, this.effectiveConfig);
    }

    public ApplicationEntity getMetadata() {
        return metadata;
    }
}