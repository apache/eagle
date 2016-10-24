/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.publisher;

public class PublishConstants {
    public static final String NOTIFICATION_TYPE = "type";
    public static final String EMAIL_NOTIFICATION = "email";
    public static final String KAFKA_STORE = "kafka";
    public static final String EAGLE_STORE = "eagleStore";

    // email specific constants
    public static final String SUBJECT = "subject";
    public static final String SENDER = "sender";
    public static final String RECIPIENTS = "recipients";
    public static final String TEMPLATE = "template";

    // kafka specific constants
    public static final String TOPIC = "topic";
    public static final String BROKER_LIST = "kafka_broker";
    public static final String WRITE_MODE = "kafka_write_mode";

    // slack specific constants
    public static final String TOKEN = "token";
    public static final String CHANNELS = "channels";
    public static final String SEVERITYS = "severitys";
    public static final String URL_TEMPLATE = "urltemplate";

    public static final String ALERT_EMAIL_TIME_PROPERTY = "timestamp";
    public static final String ALERT_EMAIL_COUNT_PROPERTY = "count";
    public static final String ALERT_EMAIL_ALERTLIST_PROPERTY = "alertList";
    public static final String ALERT_EMAIL_ORIGIN_PROPERTY = "alertEmailOrigin";

    public static final String ALERT_EMAIL_MESSAGE = "alertMessage";
    public static final String ALERT_EMAIL_STREAM = "streamId";
    public static final String ALERT_EMAIL_TIMESTAMP = "alertTime";
    public static final String ALERT_EMAIL_POLICY = "policyId";
    public static final String ALERT_EMAIL_CREATOR = "creator";

}
