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
package org.apache.eagle.storage.hbase.query.coprocessor;

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.storage.hbase.query.coprocessor.generated.AggregateProtos;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The protocol adapter for migrating from hbase-0.94 to hbase-0.96+
 *
 * @since 6/3/15
 */
public final class ProtoBufConverter {
    public static AggregateResult fromPBResult(AggregateProtos.AggregateResult pbResult) throws IOException {
        ByteArrayDataInput byteArrayDataInput = ByteStreams.newDataInput(pbResult.getByteArray().toByteArray());;
        AggregateResult result = new AggregateResult();
        result.readFields(byteArrayDataInput);
        return result;
    }

    public static AggregateProtos.AggregateRequest toPBRequest(EntityDefinition entityDefinition, Scan scan, List<String> groupbyFields, List<byte[]> aggregateFuncTypesBytes, List<String> aggregatedFields) throws IOException {
        AggregateProtos.AggregateRequest.Builder builder = AggregateProtos.AggregateRequest.newBuilder()
                .setEntityDefinition(AggregateProtos.EntityDefinition.newBuilder().setByteArray(writableToByteString(entityDefinition)))
                .setScan(toPBScan(scan));

        for(String groupbyField:groupbyFields) builder.addGroupbyFields(groupbyField);
        for(byte[] funcTypeBytes:aggregateFuncTypesBytes) builder.addAggregateFuncTypes(ByteString.copyFrom(funcTypeBytes));
        for(String aggField:aggregatedFields) builder.addAggregatedFields(aggField);

        return builder.build();
    }

    public static ByteString writableToByteString(Writable writable) throws IOException {
        ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput();;
        writable.write(dataOutput);
        return ByteString.copyFrom(dataOutput.toByteArray());
    }

    public static AggregateProtos.TimeSeriesAggregateRequest toPBTimeSeriesRequest(EntityDefinition entityDefinition, Scan scan, List<String> groupbyFields, List<byte[]> aggregateFuncTypesBytes, List<String> aggregatedFields, long startTime, long endTime, long intervalMin) throws IOException {
        AggregateProtos.TimeSeriesAggregateRequest.Builder builder = AggregateProtos.TimeSeriesAggregateRequest.newBuilder()
                .setEntityDefinition(AggregateProtos.EntityDefinition.newBuilder().setByteArray(writableToByteString(entityDefinition)))
                .setScan(toPBScan(scan));

        for(String groupbyField:groupbyFields) builder.addGroupbyFields(groupbyField);
        for(byte[] funcTypeBytes:aggregateFuncTypesBytes) builder.addAggregateFuncTypes(ByteString.copyFrom(funcTypeBytes));
        for(String aggField:aggregatedFields) builder.addAggregatedFields(aggField);

        builder.setStartTime(startTime);
        builder.setEndTime(endTime);
        builder.setIntervalMin(intervalMin);

        return builder.build();
    }

    public static EntityDefinition fromPBEntityDefinition(AggregateProtos.EntityDefinition entityDefinition) throws IOException {
        ByteArrayDataInput byteArrayDataInput = ByteStreams.newDataInput(entityDefinition.getByteArray().toByteArray());;
        EntityDefinition result = new EntityDefinition();
        result.readFields(byteArrayDataInput);
        return result;
    }

    public static List<String> fromPBStringList(com.google.protobuf.ProtocolStringList groupbyFieldsList) {
        List<String> result = new ArrayList<>(groupbyFieldsList.size());
        for(ByteString byteString:groupbyFieldsList.asByteStringList()){
            result.add(byteString.toStringUtf8());
        }
        return result;
    }

    public static List<byte[]> fromPBByteArrayList(List<ByteString> aggregateFuncTypesList) {
        List<byte[]> bytesArrayList = new ArrayList<>(aggregateFuncTypesList.size());
        for(ByteString byteString:aggregateFuncTypesList){
            bytesArrayList.add(byteString.toByteArray());
        }
        return bytesArrayList;
    }

    /**
     *
     * @param scan
     * @return
     */
    public static Scan fromPBScan(ClientProtos.Scan scan) throws IOException {
        return ProtobufUtil.toScan(scan);
    }

    public static ClientProtos.Scan toPBScan(Scan scan) throws IOException {
        return ProtobufUtil.toScan(scan);
    }

    public static AggregateProtos.AggregateResult toPBAggregateResult(AggregateResult result) throws IOException {
        ByteArrayDataOutput output = ByteStreams.newDataOutput();
        result.write(output);
        return AggregateProtos.AggregateResult.newBuilder()
                .setByteArray(ByteString.copyFrom(output.toByteArray()))
                .build();
    }
}