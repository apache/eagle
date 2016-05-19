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
package org.apache.eagle.security.oozie.parse;

import org.apache.commons.lang.StringUtils;
import org.apache.eagle.common.DateTimeUtil;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OozieAuditLogParser {

    public static final String MESSAGE_SPLIT_FLAG = "( - )";
    private static final String COMMON_REGEX = "\\s([^\\]]*\\])";
    public static final String ALLOW_ALL_REGEX = "(.*)";
    private static final String TIMESTAMP_REGEX = "(\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d,\\d\\d\\d)";
    private static final String WHITE_SPACE_REGEX = "\\s+";
    private static final String LOG_LEVEL_REGEX = "(\\w+)";
    private static final String OOZIEAUDIT_FLAG = "(\\w+:\\d+)";
    private static final String PREFIX_REGEX = TIMESTAMP_REGEX + WHITE_SPACE_REGEX + LOG_LEVEL_REGEX
            + WHITE_SPACE_REGEX;
    private final static String IP = "IP";
    private final static String USER = "USER";
    private final static String GROUP = "GROUP";
    private final static String APP = "APP";
    private final static String JOBID = "JOBID";
    private final static String OPERATION = "OPERATION";
    private final static String PARAMETER = "PARAMETER";
    private final static String STATUS = "STATUS";
    private final static String HTTPCODE = "HTTPCODE";
    private final static String ERRORCODE = "ERRORCODE";
    private final static String ERRORMESSAGE = "ERRORMESSAGE";
    private static final Pattern LOG_PATTERN = constructPattern();

    public OozieAuditLogObject parse(String logLine) throws Exception {

        OozieAuditLogObject oozieAuditLogObject = new OozieAuditLogObject();
        Matcher matcher = LOG_PATTERN.matcher(logLine);
        if (!matcher.matches()) {
            return null;
        }
        applyValueTo(oozieAuditLogObject, matcher);

        return oozieAuditLogObject;
    }


    private static Pattern constructPattern() {
        List<String> patterns = new ArrayList<String>(11);
        patterns.add(IP);
        patterns.add(USER);
        patterns.add(GROUP);
        patterns.add(APP);
        patterns.add(JOBID);
        patterns.add(OPERATION);
        patterns.add(PARAMETER);
        patterns.add(STATUS);
        patterns.add(HTTPCODE);
        patterns.add(ERRORCODE);
        patterns.add(ERRORMESSAGE);

        StringBuilder sb = new StringBuilder();
        sb.append(PREFIX_REGEX + OOZIEAUDIT_FLAG);
        sb.append(MESSAGE_SPLIT_FLAG);
        for (int i = 0; i < patterns.size(); i++) {
          /*  sb.append("(");
            sb.append(patterns.get(i) + "(" + COMMON_REGEX + ")");
            sb.append(")");
            sb.append(ALLOW_ALL_REGEX);*/
            sb.append("(");
            sb.append(patterns.get(i) + COMMON_REGEX);
            sb.append(")");
            sb.append(ALLOW_ALL_REGEX);
        }
        String rs = StringUtils.removeEnd(sb.toString(), ALLOW_ALL_REGEX);
        return Pattern.compile(rs);
    }

    private void applyValueTo(OozieAuditLogObject oozieAuditLogObject, Matcher matcher) throws ParseException {
        oozieAuditLogObject.timestamp = DateTimeUtil.humanDateToMilliseconds(matcher.group(1));
        oozieAuditLogObject.level = matcher.group(2);
        oozieAuditLogObject.ip = StringUtils.removeEnd(StringUtils.removeStart(matcher.group(6), "["), "]");
        oozieAuditLogObject.user = StringUtils.removeEnd(StringUtils.removeStart(matcher.group(9), "["), "]");
        oozieAuditLogObject.group = StringUtils.removeEnd(StringUtils.removeStart(matcher.group(12), "["), "]");
        oozieAuditLogObject.app = StringUtils.removeEnd(StringUtils.removeStart(matcher.group(15), "["), "]");
        oozieAuditLogObject.jobId = StringUtils.removeEnd(StringUtils.removeStart(matcher.group(18), "["), "]");
        oozieAuditLogObject.operation = StringUtils.removeEnd(StringUtils.removeStart(matcher.group(21), "["), "]");
        oozieAuditLogObject.parameter = StringUtils.removeEnd(StringUtils.removeStart(matcher.group(24), "["), "]");
        oozieAuditLogObject.status = StringUtils.removeEnd(StringUtils.removeStart(matcher.group(27), "["), "]");
        oozieAuditLogObject.httpcode = StringUtils.removeEnd(StringUtils.removeStart(matcher.group(30), "["), "]");
        oozieAuditLogObject.errorcode = StringUtils.removeEnd(StringUtils.removeStart(matcher.group(33), "["), "]");
        oozieAuditLogObject.errormessage = StringUtils.removeEnd(StringUtils.removeStart(matcher.group(36), "["), "]");
    }


}
