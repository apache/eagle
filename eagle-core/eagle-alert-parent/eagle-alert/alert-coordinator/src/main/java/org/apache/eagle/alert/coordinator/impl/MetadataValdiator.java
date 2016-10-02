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
package org.apache.eagle.alert.coordinator.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.coordinator.IScheduleContext;
import org.apache.eagle.alert.coordinator.ValidateState;
import org.apache.eagle.alert.coordinator.provider.InMemScheduleConext;
import org.apache.eagle.alert.coordinator.provider.ScheduleContextBuilder;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 10/1/16.
 */
public class MetadataValdiator {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataValdiator.class);

    private static final Map<StreamColumn.Type, String> _EAGLE_SIDDHI_TYPE_MAPPING = new HashMap<>();

    static {
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.Type.STRING, "STRING");
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.Type.INT, "INT");
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.Type.LONG, "LONG");
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.Type.FLOAT, "FLOAT");
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.Type.DOUBLE, "DOUBLE");
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.Type.BOOL, "BOOL");
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.Type.OBJECT, "OBJECT");
    }

    private IScheduleContext context;
    private final ValidateState state;

    public MetadataValdiator(IMetadataServiceClient client) {
        List<Topology> topologies = client.listTopologies();
        List<Kafka2TupleMetadata> datasources = client.listDataSources();
        List<StreamDefinition> streams = client.listStreams();
        // filter out disabled policies
        List<PolicyDefinition> enabledPolicies = client.listPolicies();
        List<Publishment> publishments = client.listPublishment();

        context = new InMemScheduleConext(ScheduleContextBuilder.listToMap(topologies), new HashMap<>(),
            ScheduleContextBuilder.listToMap(datasources),
            ScheduleContextBuilder.listToMap(enabledPolicies),
            ScheduleContextBuilder.listToMap(publishments),
            ScheduleContextBuilder.listToMap(streams), new HashMap<>(), new HashMap<>());
        this.state = new ValidateState();
    }

    public MetadataValdiator(IScheduleContext context) {
        this.context = context;
        this.state = new ValidateState();
    }


    public ValidateState validate() {

        validateTopology();

        validateDataSources();

        validateStreams();

        validatePolicies();

        validatePublishments();

        return state;
    }

    private void validatePolicies() {
        Collection<Publishment> pubs = context.getPublishments().values();
        for (PolicyDefinition pd : context.getPolicies().values()) {
            if (!pubs.stream().anyMatch(p -> p.getPolicyIds().contains(pd.getName()))) {
                state.appendUnPublishedPolicies(pd.getName());
            }

            boolean isStreamMiss = false;
            StringBuilder builder = new StringBuilder();
            for (String inputStream : pd.getInputStreams()) {
                if (context.getStreamSchemas().get(inputStream) == null) {
                    state.appendPublishemtnValidation(pd.getName(), String.format("policy %s contains unknown stream %s!", pd.getName(), inputStream));
                    isStreamMiss = true;
                    break;
                }
                builder.append(buildStreamDefinition(context.getStreamSchemas().get(inputStream)));
                builder.append("\n");
            }

            if (isStreamMiss) {
                continue;
            }
            builder.append(pd.getDefinition().getValue());

            // now evaluate
            try {
                SiddhiManager sm = new SiddhiManager();
                sm.createExecutionPlanRuntime(builder.toString());
            } catch (Exception e) {
                LOG.error(String.format("siddhi creation failed! %s ", builder.toString()), e);
                state.appendPolicyValidation(pd.getName(), e.getMessage());
            }
        }
    }

    private String buildStreamDefinition(StreamDefinition streamDefinition) {
        List<String> columns = new ArrayList<>();
        if (streamDefinition.getColumns() != null) {
            for (StreamColumn column : streamDefinition.getColumns()) {
                columns.add(String.format("%s %s", column.getName(), _EAGLE_SIDDHI_TYPE_MAPPING.get(column.getType().toString().toLowerCase())));
            }
        } else {
            LOG.warn("No columns found for stream {}" + streamDefinition.getStreamId());
        }
        return String.format("define stream %s( %s );", streamDefinition.getStreamId(), StringUtils.join(columns, ","));
    }


    private void validatePublishments() {
        Collection<PolicyDefinition> definitions = context.getPolicies().values();

        for (Publishment p : context.getPublishments().values()) {
            //TODO: check type; check serializer types; check dedup fields existence; check extend deduplicator...
            Set<String> unknown = p.getPolicyIds().stream().filter(pid -> definitions.stream().anyMatch(pd -> pd.getName().equals(pid))).collect(Collectors.toSet());
            if (unknown.size() > 0) {
                state.appendPublishemtnValidation(p.getName(), String.format("publishment %s reference unknown/uneabled policy %s!", p.getName(), unknown));
            }
        }
    }

    private void validateStreams() {
        Collection<Kafka2TupleMetadata> datasources = context.getDataSourceMetadata().values();
        Collection<PolicyDefinition> definitions = context.getPolicies().values();
        for (StreamDefinition sd : context.getStreamSchemas().values()) {
            if (!datasources.stream().anyMatch(d -> d.getName().equals(sd.getDataSource()))) {
                state.appendStreamValidation(sd.getStreamId(), String.format("stream %s reference unknown data source %s !", sd.getStreamId(), sd.getDataSource()));
            }
            if (!definitions.stream().anyMatch(p -> p.getInputStreams().contains(sd.getStreamId()))) {
                state.appendUnusedStreams(sd.getStreamId());
            }
            // more on columns
            if (sd.getColumns() == null || sd.getColumns().size() == 0) {
                state.appendStreamValidation(sd.getStreamId(), String.format("stream %s have empty columns!", sd.getStreamId()));
            }
        }
    }

    private void validateDataSources() {
        Collection<StreamDefinition> sds = context.getStreamSchemas().values();
        for (Kafka2TupleMetadata ds : context.getDataSourceMetadata().values()) {
            // simply do a O(^2) loop
            if (!sds.stream().anyMatch(t -> t.getDataSource().equals(ds.getName()))) {
                state.appendUnusedDatasource(ds.getName());
            }

            if (!"KAFKA".equalsIgnoreCase(ds.getType())) {
                state.appendDataSourceValidation(ds.getName(), String.format(" unsupported data source type %s !", ds.getType()));
            }

            //scheme
            //            String schemeCls = ds.getSchemeCls();
            //            try {
            //                Object scheme = Class.forName(schemeCls).getConstructor(String.class, Map.class).newInstance(ds.getTopic(), new HashMap<>());// coul only mock empty map
            //                if (!(scheme instanceof MultiScheme || scheme instanceof Scheme)) {
            //                    throw new IllegalArgumentException(" scheme class not subclass of Scheme or MultiScheme !");
            //                }
            //            } catch (Exception e) {
            //                state.appendDataSourceValidation(ds.getName(), String.format("schemeCls %s expected to be qualified sub class name of %s or %s with given constructor signature!"
            //                      +"Message: %s !",
            //                    schemeCls, Scheme.class.getCanonicalName(), MultiScheme.class.getCanonicalName(), e.getMessage()));
            //            }

            // codec
            if (ds.getCodec() == null) {
                state.appendDataSourceValidation(ds.getName(), String.format("codec of datasource must *not* be null!"));
                continue;
            }
            //            String selectCls = ds.getCodec().getStreamNameSelectorCls();
            //            try {
            //                StreamNameSelector cachedSelector = (StreamNameSelector) Class.forName(selectCls).getConstructor(Properties.class)
            //                    .newInstance(ds.getCodec().getStreamNameSelectorProp());
            //            } catch (Exception e) {
            //                state.appendDataSourceValidation(ds.getName(), String.format("streamNameSelectorCls %s expected to be subclass of %s and with given constructor signature! Message: %s !",
            //                    selectCls, StreamNameSelector.class.getCanonicalName(), e.getMessage()));
            //            }

        }
    }

    private void validateTopology() {
        for (Topology t : context.getTopologies().values()) {
        }
    }

}
