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

package org.apache.eagle.alert.notification;

import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.common.EagleBase64Wrapper;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.log.entity.HBaseInternalLogHelper;
import org.apache.eagle.log.entity.InternalLog;
import org.apache.eagle.log.entity.RowkeyBuilder;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.mortbay.util.UrlEncoded;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UrlBuilder {

    private static final Logger logger = LoggerFactory.getLogger(UrlBuilder.class);

    public static String getEncodedRowkey(AlertAPIEntity entity) throws Exception {
        InternalLog log = HBaseInternalLogHelper.convertToInternalLog(entity, EntityDefinitionManager.getEntityDefinitionByEntityClass(entity.getClass()));
        return EagleBase64Wrapper.encodeByteArray2URLSafeString(RowkeyBuilder.buildRowkey(log));
    }

    public static String buildAlertDetailUrl(String host, int port, AlertAPIEntity entity) {
        String baseUrl = "http://" + host + ":" + String.valueOf(port) + "/eagle-service/#/dam/alertDetail/";
        try {
            return baseUrl + UrlEncoded.encodeString(getEncodedRowkey(entity));
        }
        catch (Exception ex) {
            logger.error("Fail to populate encodedRowkey for alert Entity" + entity.toString());
            return "N/A";
        }
    }

    public static String buiildPolicyDetailUrl(String host, int port, Map<String, String> tags) {
        String baseUrl = "http://" + host + ":" + String.valueOf(port) + "/eagle-service/#/dam/policyDetail?";
        String format = "policy=%s&site=%s&executor=%s";
        String policy = tags.get(AlertConstants.POLICY_ID);
        String site = tags.get(EagleConfigConstants.SITE);
        String alertExecutorID = tags.get(AlertConstants.ALERT_EXECUTOR_ID);
        if (policy != null && site != null && alertExecutorID != null) {
            return baseUrl + String.format(format, policy, site, alertExecutorID);
        }
        return "N/A";
    }
}
