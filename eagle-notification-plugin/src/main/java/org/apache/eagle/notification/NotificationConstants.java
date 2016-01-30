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

package org.apache.eagle.notification;

/**
 */
public class NotificationConstants {

    public static final String NOTIFICATION_TYPE = "notificationType";

    public static final String EMAIL_NOTIFICATION_RESOURCE_NM = "Email Notification";
    public static final String KAFKA_STORE = "Kafka Store";
    // email specific constants
    public static final String SUBJECT = "subject";
    public static final String SENDER = "sender";
    public static final String RECIPIENTS = "recipients";
    public static final String TPL_FILE_NAME = "tplFileName";

    // Kafka Store Conf
    public static final String KAFKA_TOPIC = "kafkaTopic";

}
