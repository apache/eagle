/**
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
package org.apache.eagle.alert.engine.evaluator.impl;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SiddhiDefinitionAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(SiddhiDefinitionAdapter.class);
    public static final String DEFINE_STREAM_TEMPLATE = "define stream %s ( %s );";

    public static String buildStreamDefinition(StreamDefinition streamDefinition) {
        List<String> columns = new ArrayList<>();
        Preconditions.checkNotNull(streamDefinition, "StreamDefinition is null");
        if (streamDefinition.getColumns() != null) {
            for (StreamColumn column : streamDefinition.getColumns()) {
                columns.add(String.format("%s %s", column.getName(), convertToSiddhiAttributeType(column.getType()).toString().toLowerCase()));
            }
        } else {
            LOG.warn("No columns found for stream {}" + streamDefinition.getStreamId());
        }
        return String.format(DEFINE_STREAM_TEMPLATE, streamDefinition.getStreamId(), StringUtils.join(columns, ","));
    }

    public static Attribute.Type convertToSiddhiAttributeType(StreamColumn.Type type) {
        if (_EAGLE_SIDDHI_TYPE_MAPPING.containsKey(type)) {
            return _EAGLE_SIDDHI_TYPE_MAPPING.get(type);
        }

        throw new IllegalArgumentException("Unknown stream type: " + type);
    }

    public static Class<?> convertToJavaAttributeType(StreamColumn.Type type) {
        if (_EAGLE_JAVA_TYPE_MAPPING.containsKey(type)) {
            return _EAGLE_JAVA_TYPE_MAPPING.get(type);
        }

        throw new IllegalArgumentException("Unknown stream type: " + type);
    }

    public static StreamColumn.Type convertFromJavaAttributeType(Class<?> type) {
        if (_JAVA_EAGLE_TYPE_MAPPING.containsKey(type)) {
            return _JAVA_EAGLE_TYPE_MAPPING.get(type);
        }

        throw new IllegalArgumentException("Unknown stream type: " + type);
    }

    public static StreamColumn.Type convertFromSiddhiAttributeType(Attribute.Type type) {
        if (_SIDDHI_EAGLE_TYPE_MAPPING.containsKey(type)) {
            return _SIDDHI_EAGLE_TYPE_MAPPING.get(type);
        }

        throw new IllegalArgumentException("Unknown siddhi type: " + type);
    }

    public static String buildSiddhiExecutionPlan(PolicyDefinition policyDefinition, Map<String, StreamDefinition> sds) {
        StringBuilder builder = new StringBuilder();
        PolicyDefinition.Definition coreDefinition = policyDefinition.getDefinition();
        // init if not present
        if (coreDefinition.getInputStreams() == null || coreDefinition.getInputStreams().isEmpty()) {
            coreDefinition.setInputStreams(policyDefinition.getInputStreams());
        }
        if (coreDefinition.getOutputStreams() == null || coreDefinition.getOutputStreams().isEmpty()) {
            coreDefinition.setOutputStreams(policyDefinition.getOutputStreams());
        }

        for (String inputStream : coreDefinition.getInputStreams()) {
            builder.append(SiddhiDefinitionAdapter.buildStreamDefinition(sds.get(inputStream)));
            builder.append("\n");
        }
        builder.append(coreDefinition.value);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Generated siddhi execution plan: {} from definition: {}", builder.toString(), coreDefinition);
        }
        return builder.toString();
    }

    public static String buildSiddhiExecutionPlan(String policyDefinition, Map<String, StreamDefinition> inputStreamDefinitions) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String,StreamDefinition> entry: inputStreamDefinitions.entrySet()) {
            builder.append(SiddhiDefinitionAdapter.buildStreamDefinition(entry.getValue()));
            builder.append("\n");
        }
        builder.append(policyDefinition);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Generated siddhi execution plan: {}", builder.toString());
        }
        return builder.toString();
    }

    /**
     * public enum Type {
     * STRING, INT, LONG, FLOAT, DOUBLE, BOOL, OBJECT
     * }.
     */
    private static final Map<StreamColumn.Type, Attribute.Type> _EAGLE_SIDDHI_TYPE_MAPPING = new HashMap<>();
    private static final Map<StreamColumn.Type, Class<?>> _EAGLE_JAVA_TYPE_MAPPING = new HashMap<>();
    private static final Map<Class<?>, StreamColumn.Type> _JAVA_EAGLE_TYPE_MAPPING = new HashMap<>();
    private static final Map<Attribute.Type, StreamColumn.Type> _SIDDHI_EAGLE_TYPE_MAPPING = new HashMap<>();

    static {
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.Type.STRING, Attribute.Type.STRING);
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.Type.INT, Attribute.Type.INT);
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.Type.LONG, Attribute.Type.LONG);
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.Type.FLOAT, Attribute.Type.FLOAT);
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.Type.DOUBLE, Attribute.Type.DOUBLE);
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.Type.BOOL, Attribute.Type.BOOL);
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.Type.OBJECT, Attribute.Type.OBJECT);

        _EAGLE_JAVA_TYPE_MAPPING.put(StreamColumn.Type.STRING, String.class);
        _EAGLE_JAVA_TYPE_MAPPING.put(StreamColumn.Type.INT, Integer.class);
        _EAGLE_JAVA_TYPE_MAPPING.put(StreamColumn.Type.LONG, Long.class);
        _EAGLE_JAVA_TYPE_MAPPING.put(StreamColumn.Type.FLOAT, Float.class);
        _EAGLE_JAVA_TYPE_MAPPING.put(StreamColumn.Type.DOUBLE, Double.class);
        _EAGLE_JAVA_TYPE_MAPPING.put(StreamColumn.Type.BOOL, Boolean.class);
        _EAGLE_JAVA_TYPE_MAPPING.put(StreamColumn.Type.OBJECT, Object.class);

        _JAVA_EAGLE_TYPE_MAPPING.put(String.class, StreamColumn.Type.STRING);
        _JAVA_EAGLE_TYPE_MAPPING.put(Integer.class, StreamColumn.Type.INT);
        _JAVA_EAGLE_TYPE_MAPPING.put(Long.class, StreamColumn.Type.LONG);
        _JAVA_EAGLE_TYPE_MAPPING.put(Float.class, StreamColumn.Type.FLOAT);
        _JAVA_EAGLE_TYPE_MAPPING.put(Double.class, StreamColumn.Type.DOUBLE);
        _JAVA_EAGLE_TYPE_MAPPING.put(Boolean.class, StreamColumn.Type.BOOL);
        _JAVA_EAGLE_TYPE_MAPPING.put(Object.class, StreamColumn.Type.OBJECT);

        _SIDDHI_EAGLE_TYPE_MAPPING.put(Attribute.Type.STRING, StreamColumn.Type.STRING);
        _SIDDHI_EAGLE_TYPE_MAPPING.put(Attribute.Type.INT, StreamColumn.Type.INT);
        _SIDDHI_EAGLE_TYPE_MAPPING.put(Attribute.Type.LONG, StreamColumn.Type.LONG);
        _SIDDHI_EAGLE_TYPE_MAPPING.put(Attribute.Type.FLOAT, StreamColumn.Type.FLOAT);
        _SIDDHI_EAGLE_TYPE_MAPPING.put(Attribute.Type.DOUBLE, StreamColumn.Type.DOUBLE);
        _SIDDHI_EAGLE_TYPE_MAPPING.put(Attribute.Type.BOOL, StreamColumn.Type.BOOL);
        _SIDDHI_EAGLE_TYPE_MAPPING.put(Attribute.Type.OBJECT, StreamColumn.Type.OBJECT);
    }

    public static StreamDefinition convertFromSiddiDefinition(AbstractDefinition siddhiDefinition) {
        StreamDefinition streamDefinition = new StreamDefinition();
        streamDefinition.setStreamId(siddhiDefinition.getId());
        List<StreamColumn> columns = new ArrayList<>(siddhiDefinition.getAttributeNameArray().length);
        for (Attribute attribute : siddhiDefinition.getAttributeList()) {
            StreamColumn column = new StreamColumn();
            column.setType(convertFromSiddhiAttributeType(attribute.getType()));
            column.setName(attribute.getName());
            columns.add(column);
        }
        streamDefinition.setColumns(columns);
        streamDefinition.setTimeseries(true);
        streamDefinition.setDescription("Auto-generated stream schema from siddhi for " + siddhiDefinition.getId());
        return streamDefinition;
    }
}