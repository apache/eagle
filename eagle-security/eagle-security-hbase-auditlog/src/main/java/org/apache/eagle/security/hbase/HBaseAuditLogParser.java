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
package org.apache.eagle.security.hbase;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.security.util.LogParseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HBaseAuditLogParser implements Serializable {
    private final static Logger LOG = LoggerFactory.getLogger(HBaseAuditLogParser.class);

    private final static int LOGDATE_INDEX = 1;
    private final static int LOGLEVEL_INDEX = 2;
    private final static int LOGATTRS_INDEX = 3;
    private final static String ALLOWED = "allowed";
    private final static String DENIED = "denied";
    private final static Pattern loggerPattern = Pattern.compile("^([\\d\\s\\-:,]+)\\s+(\\w+)\\s+(.*)");
    private final static Pattern loggerContextPattern = Pattern.compile("\\w+:\\s*\\((.*)\\s*\\)");
    private final static Pattern allowedPattern = Pattern.compile(ALLOWED);


    public HBaseAuditLogObject parse(String logLine) {
        if(logLine == null || logLine.isEmpty()) return null;

        HBaseAuditLogObject ret = new HBaseAuditLogObject();
        String timestamp = "";
        String user = "";
        String scope = "";
        String action = "";
        String ip = "";
        String request = "";
        String family = "";
        String context = "";

        Matcher loggerMatcher = loggerPattern.matcher(logLine);
        if(loggerMatcher.find()) {
            try {
                timestamp = loggerMatcher.group(LOGDATE_INDEX);
                String [] attrs = loggerMatcher.group(LOGATTRS_INDEX).split(";");
                ret.status = allowedPattern.matcher(attrs[0]).find() ? ALLOWED : DENIED;
                try {
                    ip = attrs[2].split(":")[1].trim();
                } catch (Exception e) {
                    ip = "";
                }
                try {
                    request = attrs[3].split(":")[1].trim();
                } catch (Exception e) {
                    request = "";
                }
                try {
                    context = attrs[4].trim();
                } catch (Exception e) {
                    context = "";
                }

                Matcher contextMatcher = loggerContextPattern.matcher(context.replaceAll("\\s+",""));
                if(contextMatcher.find()) {
                    boolean paramsOpen = false;

                    List<String> kvs = new LinkedList<String>(Arrays.asList(contextMatcher.group(1).split(",")));

                    while (!kvs.isEmpty()) {
                        String kv = kvs.get(0);

                        if (kv.split("=").length < 2) {
                            kvs.remove(0);
                            continue;
                        }

                        String k = kv.split("=")[0];
                        String v = kv.split("=")[1];

                        if (paramsOpen && kv.substring(kv.length() - 1).equals("]")) {
                            paramsOpen = false;
                            v = v.substring(0, v.length() - 1);
                        }

                        switch (k) {
                            case "user":
                                user = v;
                                break;
                            case "scope":
                                scope = v;
                                break;
                            case "family":
                                family = v;
                                break;
                            case "action":
                                action = v;
                                break;
                            case "params":
                                kvs.add(v.substring(1) + "=" + kv.split("=")[2]);
                                paramsOpen = true;
                                break;
                            default: break;
                        }

                        kvs.remove(0);
                    }
                }

                if(StringUtils.isNotEmpty(family)) {
                    if(!scope.contains(":")) scope = "default:" + scope;
                    scope = String.format("%s:%s", scope, family);
                }
                if(StringUtils.isNotEmpty(ip)) {
                    ret.host = ip.substring(1);
                }
                ret.timestamp = DateTimeUtil.humanDateToMilliseconds(timestamp);
                ret.scope = scope;
                ret.action = action;
                ret.user = LogParseUtil.parseUserFromUGI(user);
                ret.request = request;
                return ret;
            } catch(Exception e) {
                LOG.error("Got exception when parsing audit log:" + logLine + ", exception:" + e.getMessage(), e);
            }
        }
        return null;
    }
}

