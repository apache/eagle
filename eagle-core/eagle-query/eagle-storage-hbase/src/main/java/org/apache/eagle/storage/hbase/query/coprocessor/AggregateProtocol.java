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
import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;
import java.util.List;


/**
 * TODO: replace with google-protobuf RPC when HBase is upgraded to version 0.96+
 */
public interface AggregateProtocol {

	/**
	 *
	 *
	 * @param entityDefinition
	 * @param scan
	 * @param groupbyFields
	 * @param aggregateFuncTypes
	 * @param aggregatedFields
	 * @return AggregateResult
	 * @throws java.io.IOException
	 */
	AggregateResult aggregate(EntityDefinition entityDefinition,
                              Scan scan,
                              List<String> groupbyFields,
                              List<byte[]> aggregateFuncTypes,
                              List<String> aggregatedFields) throws IOException;

	/**
	 *
	 * @param entityDefinition
	 * @param scan
	 * @param groupbyFields
	 * @param aggregateFuncTypes
	 * @param aggregatedFields
	 * @param intervalMin
	 * @return AggregateResult
	 * @throws java.io.IOException
	 */
	AggregateResult aggregate(EntityDefinition entityDefinition,
                              Scan scan,
                              List<String> groupbyFields,
                              List<byte[]> aggregateFuncTypes,
                              List<String> aggregatedFields,
                              long startTime,
                              long endTime,
                              long intervalMin) throws IOException;
}