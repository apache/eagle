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
package org.apache.eagle.flink;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class SiddhiDefinitionAdapter {
    private static final String DEFINE_STREAM_TEMPLATE = "define stream %s ( %s );";
    private static final String OUTPUT_STREAM_TEMPLATE = "select %s insert into %s;";

    public static String buildSiddhiOutpuStreamClause(String outStreamId, List<StreamColumn> columns){
        List<String> siddhiColumns = new ArrayList<>();
        for (StreamColumn column : columns) {
            siddhiColumns.add(column.getName());
        }
        return String.format(OUTPUT_STREAM_TEMPLATE, StringUtils.join(siddhiColumns, ","), outStreamId);
    }

    public static String buildSiddhiDefineStreamClause(String inStreamId, List<StreamColumn> columns){
        List<String> siddhiColumns = new ArrayList<>();
        for (StreamColumn column : columns) {
            siddhiColumns.add(String.format("%s %s", column.getName(),
                    convertToSiddhiAttributeType(column.getType()).toString().toLowerCase()));
        }
        return String.format(DEFINE_STREAM_TEMPLATE, inStreamId, StringUtils.join(siddhiColumns, ","));
    }

    public static String buildStreamDefinition(StreamDefinition streamDefinition) {
        List<String> columns = new ArrayList<>();
        Preconditions.checkNotNull(streamDefinition, "StreamDefinition is null");
        if (streamDefinition.getColumns() != null) {
            for (StreamColumn column : streamDefinition.getColumns()) {
                columns.add(String.format("%s %s", column.getName(), convertToSiddhiAttributeType(column.getType()).toString().toLowerCase()));
            }
        } else {
            log.warn("No columns found for stream {}" + streamDefinition.getStreamId());
        }
        return String.format(DEFINE_STREAM_TEMPLATE, streamDefinition.getStreamId(), StringUtils.join(columns, ","));
    }

    public static Attribute.Type convertToSiddhiAttributeType(StreamColumn.ColumnType type) {
        if (_EAGLE_SIDDHI_TYPE_MAPPING.containsKey(type)) {
            return _EAGLE_SIDDHI_TYPE_MAPPING.get(type);
        }

        throw new IllegalArgumentException("Unknown stream type: " + type);
    }

    public static Class<?> convertToJavaAttributeType(StreamColumn.ColumnType type) {
        if (_EAGLE_JAVA_TYPE_MAPPING.containsKey(type)) {
            return _EAGLE_JAVA_TYPE_MAPPING.get(type);
        }

        throw new IllegalArgumentException("Unknown stream type: " + type);
    }

    public static StreamColumn.ColumnType convertFromJavaAttributeType(Class<?> type) {
        if (_JAVA_EAGLE_TYPE_MAPPING.containsKey(type)) {
            return _JAVA_EAGLE_TYPE_MAPPING.get(type);
        }

        throw new IllegalArgumentException("Unknown stream type: " + type);
    }

    public static StreamColumn.ColumnType convertFromSiddhiAttributeType(Attribute.Type type) {
        if (_SIDDHI_EAGLE_TYPE_MAPPING.containsKey(type)) {
            return _SIDDHI_EAGLE_TYPE_MAPPING.get(type);
        }

        throw new IllegalArgumentException("Unknown siddhi type: " + type);
    }

    /**
     * Build Siddhi execution plan off a single input stream and output stream
     * A Siddhi execution plan consists of three parts: input stream definitions, output stream definitions and policy
     * So the evaluation flow is:
     *  input stream -> policy evaluation -> output stream
     */
    public static String buildSiddhiExecutionPlan(StreamDefinition inStreamDef,
                                                  String policy, StreamDefinition outStreamDef) {
        StringBuilder builder = new StringBuilder();
        List<StreamColumn> modifiedIn = new ArrayList<>(inStreamDef.getColumns());
        StreamColumn iCollectorCol = StreamColumn.builder().name("__collector__").
                type(StreamColumn.ColumnType.OBJECT).build();
        modifiedIn.add(0, iCollectorCol);
        builder.append(SiddhiDefinitionAdapter.buildSiddhiDefineStreamClause(inStreamDef.getStreamId(), modifiedIn));
        builder.append("\n");

        // concatenate policy and output stream definition
        // ex: "from sampleStream_1[name == \"cpu\" and value > 50.0] select __collector__, name, host, flag, value insert into outputStream;"
        builder.append("from ");
        builder.append(inStreamDef.getStreamId());
        builder.append(" [");
        builder.append(policy);
        builder.append("] ");

        List<StreamColumn> modifiedOut = new ArrayList<>(inStreamDef.getColumns());
        StreamColumn oCollectorCol = StreamColumn.builder().name("__collector__").
                type(StreamColumn.ColumnType.OBJECT).build();
        modifiedOut.add(0, oCollectorCol);
        builder.append(SiddhiDefinitionAdapter.buildSiddhiOutpuStreamClause(outStreamDef.getStreamId(), modifiedOut));
        log.debug("Generated siddhi execution plan: {}", builder.toString());
        return builder.toString();
    }

    public static String buildSiddhiExecutionPlan(PolicyDefinition policyDefinition, Map<String, StreamDefinition> sds) {
        StringBuilder builder = new StringBuilder();
        PolicyDefinition.Definition coreDefinition = policyDefinition.getDefinition();
        // init if not present
        List<String> inputStreams = coreDefinition.getInputStreams();
        if (inputStreams == null || inputStreams.isEmpty()) {
            inputStreams = policyDefinition.getInputStreams();
        }

        for (String inputStream : inputStreams) {
            StreamDefinition sd = sds.get(inputStream);
            List<StreamColumn> columns = sd.getColumns();
            columns = new ArrayList<>(columns);
            StreamColumn collectorCol = StreamColumn.builder().name("__collector__").
                    type(StreamColumn.ColumnType.OBJECT).build();
            columns.add(0, collectorCol);
            builder.append(SiddhiDefinitionAdapter.buildSiddhiDefineStreamClause(sd.getStreamId(), columns));
            builder.append("\n");
        }
        builder.append(coreDefinition.value);
        log.debug("Generated siddhi execution plan: {} from definition: {}", builder.toString(), coreDefinition);
        return builder.toString();
    }

    public static String buildSiddhiExecutionPlan(String policyDefinition, Map<String, StreamDefinition> inputStreamDefinitions) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String,StreamDefinition> entry: inputStreamDefinitions.entrySet()) {
            builder.append(SiddhiDefinitionAdapter.buildStreamDefinition(entry.getValue()));
            builder.append("\n");
        }
        builder.append(policyDefinition);
        log.debug("Generated siddhi execution plan: {}", builder.toString());
        return builder.toString();
    }

    /**
     * public enum Type {
     * STRING, INT, LONG, FLOAT, DOUBLE, BOOL, OBJECT
     * }.
     */
    private static final Map<StreamColumn.ColumnType, Attribute.Type> _EAGLE_SIDDHI_TYPE_MAPPING = new HashMap<>();
    private static final Map<StreamColumn.ColumnType, Class<?>> _EAGLE_JAVA_TYPE_MAPPING = new HashMap<>();
    private static final Map<Class<?>, StreamColumn.ColumnType> _JAVA_EAGLE_TYPE_MAPPING = new HashMap<>();
    private static final Map<Attribute.Type, StreamColumn.ColumnType> _SIDDHI_EAGLE_TYPE_MAPPING = new HashMap<>();

    static {
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.ColumnType.STRING, Attribute.Type.STRING);
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.ColumnType.INT, Attribute.Type.INT);
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.ColumnType.LONG, Attribute.Type.LONG);
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.ColumnType.FLOAT, Attribute.Type.FLOAT);
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.ColumnType.DOUBLE, Attribute.Type.DOUBLE);
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.ColumnType.BOOL, Attribute.Type.BOOL);
        _EAGLE_SIDDHI_TYPE_MAPPING.put(StreamColumn.ColumnType.OBJECT, Attribute.Type.OBJECT);

        _EAGLE_JAVA_TYPE_MAPPING.put(StreamColumn.ColumnType.STRING, String.class);
        _EAGLE_JAVA_TYPE_MAPPING.put(StreamColumn.ColumnType.INT, Integer.class);
        _EAGLE_JAVA_TYPE_MAPPING.put(StreamColumn.ColumnType.LONG, Long.class);
        _EAGLE_JAVA_TYPE_MAPPING.put(StreamColumn.ColumnType.FLOAT, Float.class);
        _EAGLE_JAVA_TYPE_MAPPING.put(StreamColumn.ColumnType.DOUBLE, Double.class);
        _EAGLE_JAVA_TYPE_MAPPING.put(StreamColumn.ColumnType.BOOL, Boolean.class);
        _EAGLE_JAVA_TYPE_MAPPING.put(StreamColumn.ColumnType.OBJECT, Object.class);

        _JAVA_EAGLE_TYPE_MAPPING.put(String.class, StreamColumn.ColumnType.STRING);
        _JAVA_EAGLE_TYPE_MAPPING.put(Integer.class, StreamColumn.ColumnType.INT);
        _JAVA_EAGLE_TYPE_MAPPING.put(Long.class, StreamColumn.ColumnType.LONG);
        _JAVA_EAGLE_TYPE_MAPPING.put(Float.class, StreamColumn.ColumnType.FLOAT);
        _JAVA_EAGLE_TYPE_MAPPING.put(Double.class, StreamColumn.ColumnType.DOUBLE);
        _JAVA_EAGLE_TYPE_MAPPING.put(Boolean.class, StreamColumn.ColumnType.BOOL);
        _JAVA_EAGLE_TYPE_MAPPING.put(Object.class, StreamColumn.ColumnType.OBJECT);

        _SIDDHI_EAGLE_TYPE_MAPPING.put(Attribute.Type.STRING, StreamColumn.ColumnType.STRING);
        _SIDDHI_EAGLE_TYPE_MAPPING.put(Attribute.Type.INT, StreamColumn.ColumnType.INT);
        _SIDDHI_EAGLE_TYPE_MAPPING.put(Attribute.Type.LONG, StreamColumn.ColumnType.LONG);
        _SIDDHI_EAGLE_TYPE_MAPPING.put(Attribute.Type.FLOAT, StreamColumn.ColumnType.FLOAT);
        _SIDDHI_EAGLE_TYPE_MAPPING.put(Attribute.Type.DOUBLE, StreamColumn.ColumnType.DOUBLE);
        _SIDDHI_EAGLE_TYPE_MAPPING.put(Attribute.Type.BOOL, StreamColumn.ColumnType.BOOL);
        _SIDDHI_EAGLE_TYPE_MAPPING.put(Attribute.Type.OBJECT, StreamColumn.ColumnType.OBJECT);
    }

    public static StreamDefinition convertFromSiddiDefinition(AbstractDefinition siddhiDefinition) {
        StreamDefinition.StreamDefinitionBuilder builder = StreamDefinition.builder();
        builder.streamId(siddhiDefinition.getId());
        List<StreamColumn> columns = new ArrayList<>(siddhiDefinition.getAttributeNameArray().length);
        for (Attribute attribute : siddhiDefinition.getAttributeList()) {
            StreamColumn.StreamColumnBuilder colBuilder = StreamColumn.builder();
            colBuilder.type(convertFromSiddhiAttributeType(attribute.getType()));
            colBuilder.name(attribute.getName());
            columns.add(colBuilder.build());
        }
        builder.columns(columns);
        builder.timeseries(true);
        builder.description("Auto-generated stream schema from siddhi for " + siddhiDefinition.getId());
        return builder.build();
    }
}