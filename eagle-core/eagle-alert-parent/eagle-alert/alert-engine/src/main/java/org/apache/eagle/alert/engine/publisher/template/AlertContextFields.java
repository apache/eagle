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
package org.apache.eagle.alert.engine.publisher.template;

import java.util.Arrays;
import java.util.List;

public class AlertContextFields {
    public static final String STREAM_ID = "STREAM_ID";
    public static final String ALERT_ID = "ALERT_ID";
    public static final String CREATED_BY = "CREATED_BY";
    public static final String POLICY_ID = "POLICY_ID";
    public static final String CREATED_TIMESTAMP = "CREATED_TIMESTAMP";
    public static final String CREATED_TIME = "CREATED_TIME";
    public static final String ALERT_TIMESTAMP = "ALERT_TIMESTAMP";
    public static final String ALERT_TIME = "ALERT_TIME";
    public static final String ALERT_SCHEMA = "ALERT_SCHEMA";
    public static final String ALERT_EVENT = "ALERT_EVENT";
    public static final String POLICY_DESC = "POLICY_DESC";
    public static final String POLICY_TYPE = "POLICY_TYPE";
    public static final String POLICY_DEFINITION = "POLICY_DEFINITION";
    public static final String POLICY_HANDLER = "POLICY_HANDLER";

    public static List<String> getAllContextFields() {
        return Arrays.asList(
            STREAM_ID, ALERT_ID, CREATED_BY, POLICY_ID, CREATED_TIMESTAMP, CREATED_TIME, ALERT_TIMESTAMP, ALERT_TIME, ALERT_SCHEMA, POLICY_DESC, POLICY_TYPE, POLICY_DEFINITION, POLICY_HANDLER
        );
    }
}