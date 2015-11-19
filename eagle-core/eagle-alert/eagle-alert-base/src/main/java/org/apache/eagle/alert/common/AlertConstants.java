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
package org.apache.eagle.alert.common;

public class AlertConstants {
	public final static String ALERT_SERVICE_ENDPOINT_NAME = "AlertService";
	public final static String ALERT_DEFINITION_SERVICE_ENDPOINT_NAME = "AlertDefinitionService";
	public final static String ALERT_STREAM_SCHEMA_SERVICE_ENDPOINT_NAME = "AlertStreamSchemaService";
	public final static String ALERT_DATA_SOURCE_SERVICE_ENDPOINT_NAME = "AlertDataSourceService";
	public final static String ALERT_EXECUTOR_SERVICE_ENDPOINT_NAME = "AlertExecutorService";
	public final static String ALERT_STREAM_SERVICE_ENDPOINT_NAME = "AlertStreamService";
	public static final String ALERT_EMAIL_ORIGIN_PROPERTY = "alertEmailOrigin";
	public static final String ALERT_TIMESTAMP_PROPERTY = "alertTimestamp";

	public static final String ALERT_EMAIL_TIME_PROPERTY = "timestamp";
	public static final String ALERT_EMAIL_COUNT_PROPERTY = "count";
	public static final String ALERT_EMAIL_ALERTLIST_PROPERTY = "alertList";

	public static final String URL = "url";
	public static final String ALERT_SOURCE = "alertSource";
	public static final String ALERT_MESSAGE = "alertMessage";
	public static final String SUBJECT = "subject";
	public static final String ALERT_EXECUTOR_ID = "alertExecutorId";
	public static final String POLICY_NAME = "policyName";
	public static final String POLICY_ID = "policyId";
    public static final String SOURCE_STREAMS = "sourceStreams";
    public static final String ALERT_EVENT = "alertEvent";
	public static final String POLICY_DETAIL_URL = "policyDetailUrl";
	public static final String ALERT_DETAIL_URL = "alertDetailUrl";

	public static final String POLICY_DEFINITION = "policyDefinition";
	public static final String POLICY_TYPE = "policyType";
	public static final String STREAM_NAME = "streamName";
	public static final String ATTR_NAME = "attrName";

	public static final String ALERT_EXECUTOR_CONFIGS = "alertExecutorConfigs";
	public static final String PARALLELISM = "parallelism";
	public static final String PARTITIONER = "partitioner";
	public static final String SOURCE = "source";
	public static final String PARTITIONSEQ = "partitionSeq";

	public enum policyType {
		siddhiCEPEngine,
		MachineLearning;

		policyType() {
		}
	}

}
