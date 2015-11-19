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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.eagle.security.hbase.parse;

import org.apache.eagle.common.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class HbaseAuditLogParser implements Serializable {
    private final static Logger LOG = LoggerFactory.getLogger(HbaseAuditLogParser.class);

    private final static int LOGDATE_INDEX = 1;
    private final static int LOGLEVEL_INDEX = 2;
    private final static int LOGATTRS_INDEX = 3;
    private final static String LOGDATE="logdate";
    private final static String LOGLEVEL="loglevel";
    private final static String CONTROLLER = "SecurityLogger.org.apache.hadoop.hbase.security.access.AccessController";
    private final static String REASON = "reason";
    private final static String ADDRESS = "address";
    private final static String REQUEST = "request";
    private final static String ALLOWED = "allowed";
    private final static String DENIED = "denied";
    private final static String USER = "user";
    private final static String SCOPE = "scope";
    private final static String FAMILY = "family";
    private final static String ACTION = "action";
    private final static Pattern loggerPattern = Pattern.compile("^([\\d\\s\\-:,]+)\\s+(\\w+)\\s+(.*)");
    private final static Pattern loggerAttributesPattern = Pattern.compile("([\\w\\.]+:[/\\w\\.\\s\\\\]+);\\s+");
    private final static Pattern loggerContextPattern = Pattern.compile("\\((.*)\\)");
    private final static Pattern allowedPattern = Pattern.compile(ALLOWED);


    public HbaseAuditLogObject parse(String logLine) throws Exception {
        HbaseAuditLogObject ret = new HbaseAuditLogObject();
        Map<String, String> auditMap = parseAudit(logLine);
        if(auditMap == null) return null;

        String status = auditMap.get(CONTROLLER);
        if(status != "") {
            ret.status = allowedPattern.matcher(status).find() ? ALLOWED : DENIED;
        }

        String scope = auditMap.get(SCOPE);
        String family = auditMap.get(FAMILY);
        if(family != "") {
            if(!scope.contains(":")) scope = "default:" + scope;
            scope = String.format("%s:%s", scope, family);
        }
        String ip = auditMap.get(ADDRESS);
        if(ip != "") {
            ret.host = ip.substring(1);
        }
        ret.scope = scope;
        ret.action = auditMap.get(ACTION);
        ret.user = auditMap.get(USER);
        ret.request = auditMap.get(REQUEST);
        ret.timestamp = DateTimeUtil.humanDateToMilliseconds(auditMap.get(LOGDATE));
        return ret;
    }

    Map<String, String> parseContext(String logLine) {
        Matcher loggerMatcher = loggerContextPattern.matcher(logLine);
        Map<String, String> ret = new HashMap<>();
        if(loggerMatcher.find()) {
            String context = loggerMatcher.group(1);
            String [] kvs = context.split(",");
            for(String kv : kvs){
                String [] vals = kv.split("=");
                if(vals.length > 1) {
                    ret.put(vals[0].trim(), vals[1].trim());
                } else {
                    ret.put(vals[0].trim(), "");
                }
            }
        }
        return ret;
    }

    Map<String, String> parseAttribute(String logLine) {
        Map<String, String> ret = new HashMap<>();
        Matcher loggerMatcher = loggerAttributesPattern.matcher(logLine);
        while(loggerMatcher.find()) {
            String kv = loggerMatcher.group(1);
            String[] kvs = kv.split(":");
            if(kvs.length > 1) {
                ret.put(kvs[0].trim(), kvs[1].trim());
            } else {
                ret.put(kvs[0].trim(), "");
            }
        }
        return ret;
    }

    Map<String, String> parseAudit(String logLine) {
        Map<String, String> ret = null;

        Matcher loggerMatcher = loggerPattern.matcher(logLine);
        if(loggerMatcher.find()) {
            try {
                ret = new HashMap<>();
                ret.put(LOGDATE, loggerMatcher.group(LOGDATE_INDEX));
                ret.put(LOGLEVEL, loggerMatcher.group(LOGLEVEL_INDEX));
                String logAttr = loggerMatcher.group(LOGATTRS_INDEX);
                Map<String, String> attrs = parseAttribute(logAttr);
                ret.put(CONTROLLER, attrs.get(CONTROLLER));
                ret.put(REASON, attrs.get(REASON));
                ret.put(ADDRESS, attrs.get(ADDRESS));
                ret.put(REQUEST, attrs.get(REQUEST));
                Map<String, String> contextMap = parseContext(logAttr);
                ret.put(USER, contextMap.get(USER));
                ret.put(SCOPE, contextMap.get(SCOPE));
                ret.put(FAMILY, contextMap.get(FAMILY));
                ret.put(ACTION, contextMap.get(ACTION));
            } catch(IndexOutOfBoundsException e) {
                LOG.error("Got exception when parsing audit log:" + logLine + ", exception:" + e.getMessage(), e);
                ret = null;
            }
        }
        return ret;
    }
}



