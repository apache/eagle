/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.common.mail;

public class AlertEmailConstants {

    public static final String CONN_PLAINTEXT = "plaintext";
    public static final String CONN_TLS = "tls";
    public static final String CONN_SSL = "ssl";

    public static final String CONF_MAIL_HOST = "mail.smtp.host";
    public static final String CONF_MAIL_PORT = "mail.smtp.port";
    public static final String CONF_MAIL_AUTH = "mail.smtp.auth";
    public static final String CONF_AUTH_USER = "mail.username";
    public static final String CONF_AUTH_PASSWORD = "mail.password";
    public static final String CONF_MAIL_CONN = "mail.connection";
    public static final String CONF_MAIL_DEBUG = "mail.debug";

    public static final String SUBJECT = "subject";
    public static final String SENDER = "sender";
    public static final String RECIPIENTS = "recipients";
    public static final String TEMPLATE = "template";

    public static final String ALERT_EMAIL_TIME_PROPERTY = "timestamp";
    public static final String ALERT_EMAIL_COUNT_PROPERTY = "count";
    public static final String ALERT_EMAIL_ALERTLIST_PROPERTY = "alertList";
    public static final String ALERT_EMAIL_ORIGIN_PROPERTY = "alertEmailOrigin";

    public static final String EAGLE_EMAIL_SMTP_SERVER = "application.mailService.mailSmtpServer";
    public static final String EAGLE_EMAIL_SMTP_PORT = "application.mailService.mailSmtpPort";
    public static final String EAGLE_EMAIL_SMTP_CONN = "application.mailService.mailSmtpConn";
    public static final String EAGLE_EMAIL_SMTP_AUTH = "application.mailService.mailSmtpAuth";
    public static final String EAGLE_EMAIL_SMTP_USERNAME = "application.mailService.mailSmtpUsername";
    public static final String EAGLE_EMAIL_SMTP_PASSWORD = "application.mailService.mailSmtpPassword";
    public static final String EAGLE_EMAIL_SMTP_DEBUG = "application.mailService.mailSmtpDebug";


}
