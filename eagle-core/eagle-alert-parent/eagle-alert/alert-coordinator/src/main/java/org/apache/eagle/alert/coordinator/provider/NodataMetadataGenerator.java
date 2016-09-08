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
package org.apache.eagle.alert.coordinator.provider;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.Tuple2StreamMetadata;
import org.apache.eagle.alert.engine.coordinator.*;
import org.apache.eagle.alert.utils.TimePeriodUtils;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class NodataMetadataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(NodataMetadataGenerator.class);

    private static final String NODATA_ALERT_AGGR_STREAM = "nodata_alert_aggregation_stream";
    private static final String NODATA_ALERT_AGGR_OUTPUT_STREAM = "nodata_alert_aggregation_output_stream";
    private static final String NODATA_ALERT_AGGR_DATASOURCE_NAME = "nodata_alert_aggregation_ds";
    private static final String NODATA_ALERT_AGGR_OUTPUT_DATASOURCE_NAME = "nodata_alert_aggregation_output_ds";
    private static final String NODATA_ALERT_AGGR_TOPIC_NAME = "nodata_alert_aggregation";
    private static final String NODATA_ALERT_AGGR_OUTPUT_TOPIC_NAME = "nodata_alert";

    private static final String DATASOURCE_TYPE = "KAFKA";
    private static final String DATASOURCE_SCHEME_CLS = "org.apache.eagle.alert.engine.scheme.JsonScheme";

    private static final String NODATA_ALERT_AGGR_POLICY_TYPE = "nodataalert";
    private static final String NODATA_ALERT_AGGR_OUTPUT_POLICY_TYPE = "siddhi";

    private static final String JSON_STRING_STREAM_NAME_SELECTOR_CLS = "org.apache.eagle.alert.engine.scheme.JsonStringStreamNameSelector";
    private static final String STREAM_TIMESTAMP_COLUMN_NAME = "timestamp";
    private static final String STREAM_TIMESTAMP_FORMAT = "";

    private static final String KAFKA_PUBLISHMENT_TYPE = "org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher";
    private static final String EMAIL_PUBLISHMENT_TYPE = "org.apache.eagle.alert.engine.publisher.impl.AlertEmailPublisher";

    private static final String PUBLISHMENT_DEDUP_DURATION = "PT0M";
    private static final String PUBLISHMENT_SERIALIZER = "org.apache.eagle.alert.engine.publisher.impl.JsonEventSerializer";

    public NodataMetadataGenerator() {
    }

    public void execute(Config config, Map<String, StreamDefinition> streamDefinitionsMap,
                        Map<String, Kafka2TupleMetadata> kafkaSources,
                        Map<String, PolicyDefinition> policies, Map<String, Publishment> publishments) {
        Collection<StreamDefinition> streamDefinitions = streamDefinitionsMap.values();
        for (StreamDefinition streamDefinition : streamDefinitions) {
            StreamColumn columnWithNodataExpression = null;
            for (StreamColumn column : streamDefinition.getColumns()) {
                if (StringUtils.isNotBlank(column.getNodataExpression())) {
                    // has nodata alert setting, needs to generate the nodata alert policy
                    if (columnWithNodataExpression != null) {
                        columnWithNodataExpression = null;
                        LOG.warn("Only one column in one stream is allowed to configure nodata alert");
                        break;
                    }
                    columnWithNodataExpression = column;
                }
            }
            if (columnWithNodataExpression != null) {
                final String streamName = streamDefinition.getStreamId();

                // create nodata alert aggr stream
                if (streamDefinitionsMap.containsKey(NODATA_ALERT_AGGR_STREAM)) {
                    LOG.info("Nodata alert aggregation stream: {} already exists", NODATA_ALERT_AGGR_STREAM);
                } else {
                    streamDefinitionsMap.put(NODATA_ALERT_AGGR_STREAM, buildAggregationStream());
                    LOG.info("Created nodata alert aggregation stream: {}", NODATA_ALERT_AGGR_STREAM);
                }

                // create nodata alert aggr output stream
                if (streamDefinitionsMap.containsKey(NODATA_ALERT_AGGR_OUTPUT_STREAM)) {
                    LOG.info("Nodata alert aggregation output stream: {} already exists", NODATA_ALERT_AGGR_OUTPUT_STREAM);
                } else {
                    streamDefinitionsMap.put(NODATA_ALERT_AGGR_OUTPUT_STREAM, buildAggregationOutputStream());
                    LOG.info("Created nodata alert aggregation output stream: {}", NODATA_ALERT_AGGR_OUTPUT_STREAM);
                }

                // create nodata alert data source
                if (kafkaSources.containsKey(NODATA_ALERT_AGGR_DATASOURCE_NAME)) {
                    LOG.info("Stream: {} nodata alert aggregation datasource: {} already exists",
                        NODATA_ALERT_AGGR_STREAM, NODATA_ALERT_AGGR_DATASOURCE_NAME);
                } else {
                    kafkaSources.put(NODATA_ALERT_AGGR_DATASOURCE_NAME, buildAggregationDatasource());
                    LOG.info("Created nodata alert aggregation datasource {} for stream {}",
                        NODATA_ALERT_AGGR_DATASOURCE_NAME, NODATA_ALERT_AGGR_STREAM);
                }

                // create nodata alert aggregation output datasource
                if (kafkaSources.containsKey(NODATA_ALERT_AGGR_OUTPUT_DATASOURCE_NAME)) {
                    LOG.info("Stream: {} nodata alert aggregation output datasource: {} already exists",
                        NODATA_ALERT_AGGR_OUTPUT_STREAM, NODATA_ALERT_AGGR_OUTPUT_DATASOURCE_NAME);
                } else {
                    kafkaSources.put(NODATA_ALERT_AGGR_OUTPUT_DATASOURCE_NAME, buildAggregationOutputDatasource());
                    LOG.info("Created nodata alert aggregation output datasource {} for stream {}",
                        NODATA_ALERT_AGGR_DATASOURCE_NAME, NODATA_ALERT_AGGR_OUTPUT_STREAM);
                }

                // create nodata alert policy
                String policyName = streamName + "_nodata_alert";
                String nodataExpression = columnWithNodataExpression.getNodataExpression();
                String[] segments = nodataExpression.split(",");
                long windowPeriodInSeconds = TimePeriodUtils.getSecondsOfPeriod(Period.parse(segments[0]));
                if (policies.containsKey(policyName)) {
                    LOG.info("Stream: {} nodata alert policy: {} already exists", streamName, policyName);
                } else {
                    policies.put(policyName, buildDynamicNodataPolicy(
                        streamName,
                        policyName,
                        columnWithNodataExpression.getName(),
                        nodataExpression,
                        Arrays.asList(streamName)));
                    LOG.info("Created nodata alert policy {} with expression {} for stream {}",
                        policyName, nodataExpression, streamName);
                }

                // create nodata alert aggregation
                String aggrPolicyName = NODATA_ALERT_AGGR_STREAM + "_policy";
                if (policies.containsKey(aggrPolicyName)) {
                    LOG.info("Stream: {} nodata alert aggregation policy: {} already exists",
                        NODATA_ALERT_AGGR_OUTPUT_STREAM, aggrPolicyName);
                } else {
                    policies.put(aggrPolicyName, buildAggregationPolicy(
                        aggrPolicyName,
                        columnWithNodataExpression.getName(),
                        windowPeriodInSeconds));
                    LOG.info("Created nodata alert aggregation policy {} for stream {}",
                        aggrPolicyName, NODATA_ALERT_AGGR_OUTPUT_STREAM);
                }

                // create nodata alert publish
                String publishmentName = policyName + "_publish";
                if (publishments.containsKey(publishmentName)) {
                    LOG.info("Stream: {} nodata alert publishment: {} already exists", streamName, publishmentName);
                } else {
                    String kafkaBroker = config.getString("kafkaProducer.bootstrapServers");
                    publishments.put(publishmentName, buildKafkaAlertPublishment(
                        publishmentName, policyName, kafkaBroker, NODATA_ALERT_AGGR_TOPIC_NAME));
                    publishments.put(publishmentName + "_email", buildEmailAlertPublishment(config,
                        publishmentName + "_email", policyName, kafkaBroker, NODATA_ALERT_AGGR_TOPIC_NAME));
                    LOG.info("Created nodata alert publishment {} for stream {}", policyName + "_publish", streamName);
                }

                // create nodata alert aggregation publish
                String aggrPublishName = aggrPolicyName + "_publish";
                if (publishments.containsKey(aggrPublishName)) {
                    LOG.info("Stream: {} publishment: {} already exists", NODATA_ALERT_AGGR_STREAM, aggrPublishName);
                } else {
                    String kafkaBroker = config.getString("kafkaProducer.bootstrapServers");
                    publishments.put(aggrPublishName, buildKafkaAlertPublishment(
                        aggrPublishName, aggrPolicyName, kafkaBroker, NODATA_ALERT_AGGR_OUTPUT_TOPIC_NAME));
                    publishments.put(aggrPublishName + "_email", buildEmailAlertPublishment(config,
                        aggrPublishName + "_email", aggrPolicyName, kafkaBroker, NODATA_ALERT_AGGR_OUTPUT_TOPIC_NAME));
                    LOG.info("Created nodata alert publishment {} for stream {}", policyName + "_publish", streamName);
                }
            }
        }
    }

    private Kafka2TupleMetadata buildAggregationDatasource() {
        Kafka2TupleMetadata datasource = new Kafka2TupleMetadata();
        datasource.setName(NODATA_ALERT_AGGR_DATASOURCE_NAME);
        datasource.setType(DATASOURCE_TYPE);
        datasource.setSchemeCls(DATASOURCE_SCHEME_CLS);
        datasource.setTopic(NODATA_ALERT_AGGR_TOPIC_NAME);
        Tuple2StreamMetadata codec = new Tuple2StreamMetadata();
        codec.setStreamNameSelectorCls(JSON_STRING_STREAM_NAME_SELECTOR_CLS);
        codec.setTimestampColumn(STREAM_TIMESTAMP_COLUMN_NAME);
        codec.setTimestampFormat(STREAM_TIMESTAMP_FORMAT);
        Properties codecProperties = new Properties();
        codecProperties.put("userProvidedStreamName", NODATA_ALERT_AGGR_STREAM);
        codecProperties.put("streamNameFormat", "%s");
        codec.setStreamNameSelectorProp(codecProperties);
        datasource.setCodec(codec);
        return datasource;
    }

    private Kafka2TupleMetadata buildAggregationOutputDatasource() {
        Kafka2TupleMetadata datasource = new Kafka2TupleMetadata();
        datasource.setName(NODATA_ALERT_AGGR_OUTPUT_DATASOURCE_NAME);
        datasource.setType(DATASOURCE_TYPE);
        datasource.setSchemeCls(DATASOURCE_SCHEME_CLS);
        datasource.setTopic(NODATA_ALERT_AGGR_OUTPUT_TOPIC_NAME);
        Tuple2StreamMetadata codec = new Tuple2StreamMetadata();
        codec.setStreamNameSelectorCls(JSON_STRING_STREAM_NAME_SELECTOR_CLS);
        codec.setTimestampColumn(STREAM_TIMESTAMP_COLUMN_NAME);
        codec.setTimestampFormat(STREAM_TIMESTAMP_FORMAT);
        Properties codecProperties = new Properties();
        codecProperties.put("userProvidedStreamName", NODATA_ALERT_AGGR_OUTPUT_STREAM);
        codecProperties.put("streamNameFormat", "%s");
        codec.setStreamNameSelectorProp(codecProperties);
        datasource.setCodec(codec);
        return datasource;
    }

    private PolicyDefinition buildDynamicNodataPolicy(String streamName, String policyName,
                                                      String columnName, String expression, List<String> inputStream) {
        PolicyDefinition pd = new PolicyDefinition();
        PolicyDefinition.Definition def = new PolicyDefinition.Definition();
        //expression, something like "PT5S,dynamic,1,host"
        def.setValue(expression);
        def.setType(NODATA_ALERT_AGGR_POLICY_TYPE);
        pd.setDefinition(def);
        pd.setInputStreams(inputStream);
        pd.setOutputStreams(Arrays.asList(NODATA_ALERT_AGGR_STREAM));
        pd.setName(policyName);
        pd.setDescription(String.format("Nodata alert policy for stream %s", streamName));

        StreamPartition sp = new StreamPartition();
        sp.setStreamId(streamName);
        sp.setColumns(Arrays.asList(columnName));
        sp.setType(StreamPartition.Type.GROUPBY);
        pd.addPartition(sp);
        return pd;
    }

    private PolicyDefinition buildAggregationPolicy(String policyName, String columnName,
                                                    long windowPeriodInSeconds) {
        final PolicyDefinition pd = new PolicyDefinition();
        PolicyDefinition.Definition def = new PolicyDefinition.Definition();
        String siddhiQL = String.format(
            "from %s#window.timeBatch(%s sec) select eagle:collectWithDistinct(%s) as hosts, "
                + "originalStreamName as streamName group by originalStreamName insert into %s",
            NODATA_ALERT_AGGR_STREAM, windowPeriodInSeconds * 2,
            columnName, NODATA_ALERT_AGGR_OUTPUT_STREAM);
        LOG.info("Generated SiddhiQL {} for stream: {}", siddhiQL, NODATA_ALERT_AGGR_STREAM);
        def.setValue(siddhiQL);
        def.setType(NODATA_ALERT_AGGR_OUTPUT_POLICY_TYPE);
        pd.setDefinition(def);
        pd.setInputStreams(Arrays.asList(NODATA_ALERT_AGGR_STREAM));
        pd.setOutputStreams(Arrays.asList(NODATA_ALERT_AGGR_OUTPUT_STREAM));
        pd.setName(policyName);
        pd.setDescription("Nodata alert aggregation policy, used to merge alerts from multiple bolts");

        StreamPartition sp = new StreamPartition();
        sp.setStreamId(NODATA_ALERT_AGGR_STREAM);
        sp.setColumns(Arrays.asList(columnName));
        sp.setType(StreamPartition.Type.GROUPBY);
        pd.addPartition(sp);
        pd.setParallelismHint(0);
        return pd;
    }

    private Publishment buildKafkaAlertPublishment(String publishmentName, String policyName, String kafkaBroker, String topic) {
        Publishment publishment = new Publishment();
        publishment.setName(publishmentName);
        publishment.setType(KAFKA_PUBLISHMENT_TYPE);
        publishment.setPolicyIds(Arrays.asList(policyName));
        publishment.setDedupIntervalMin(PUBLISHMENT_DEDUP_DURATION);
        Map<String, String> publishmentProperties = new HashMap<String, String>();
        publishmentProperties.put("kafka_broker", kafkaBroker);
        publishmentProperties.put("topic", topic);
        publishment.setProperties(publishmentProperties);
        publishment.setSerializer(PUBLISHMENT_SERIALIZER);
        return publishment;
    }

    private Publishment buildEmailAlertPublishment(Config config,
                                                   String publishmentName, String policyName, String kafkaBroker, String topic) {
        Publishment publishment = new Publishment();
        publishment.setName(publishmentName);
        publishment.setType(EMAIL_PUBLISHMENT_TYPE);
        publishment.setPolicyIds(Arrays.asList(policyName));
        publishment.setDedupIntervalMin(PUBLISHMENT_DEDUP_DURATION);
        Map<String, String> publishmentProperties = new HashMap<String, String>();
        publishmentProperties.put("subject", String.format("Eagle Alert - %s", topic));
        publishmentProperties.put("template", "");
        publishmentProperties.put("sender", config.getString("email.sender"));
        publishmentProperties.put("recipients", config.getString("email.recipients"));
        publishmentProperties.put("mail.smtp.host", config.getString("email.mailSmtpHost"));
        publishmentProperties.put("mail.smtp.port", config.getString("email.mailSmtpPort"));
        publishmentProperties.put("connection", "plaintext");
        publishment.setProperties(publishmentProperties);
        publishment.setSerializer(PUBLISHMENT_SERIALIZER);
        return publishment;
    }

    private StreamDefinition buildAggregationStream() {
        final StreamDefinition sd = new StreamDefinition();
        StreamColumn tsColumn = new StreamColumn();
        tsColumn.setName("timestamp");
        tsColumn.setType(StreamColumn.Type.LONG);

        StreamColumn hostColumn = new StreamColumn();
        hostColumn.setName("host");
        hostColumn.setType(StreamColumn.Type.STRING);

        StreamColumn originalStreamNameColumn = new StreamColumn();
        originalStreamNameColumn.setName("originalStreamName");
        originalStreamNameColumn.setType(StreamColumn.Type.STRING);

        sd.setColumns(Arrays.asList(tsColumn, hostColumn, originalStreamNameColumn));
        sd.setDataSource(NODATA_ALERT_AGGR_DATASOURCE_NAME);
        sd.setStreamId(NODATA_ALERT_AGGR_STREAM);
        sd.setDescription("Nodata alert aggregation stream");
        return sd;
    }

    private StreamDefinition buildAggregationOutputStream() {
        final StreamDefinition sd = new StreamDefinition();
        StreamColumn hostColumn = new StreamColumn();
        hostColumn.setName("hosts");
        hostColumn.setType(StreamColumn.Type.STRING);

        StreamColumn osnColumn = new StreamColumn();
        osnColumn.setName("streamName");
        osnColumn.setType(StreamColumn.Type.STRING);

        sd.setColumns(Arrays.asList(hostColumn, osnColumn));
        sd.setDataSource(NODATA_ALERT_AGGR_OUTPUT_DATASOURCE_NAME);
        sd.setStreamId(NODATA_ALERT_AGGR_OUTPUT_STREAM);
        sd.setDescription("Nodata alert aggregation output stream");
        return sd;
    }

}
