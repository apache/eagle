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
