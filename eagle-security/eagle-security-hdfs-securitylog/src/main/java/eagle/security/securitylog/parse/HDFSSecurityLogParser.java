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

package eagle.security.securitylog.parse;


import org.apache.eagle.security.util.LogParseUtil;
import org.apache.eagle.common.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HDFSSecurityLogParser implements Serializable {
    private final static Logger LOG = LoggerFactory.getLogger(HDFSSecurityLogParser.class);

    private final static int LOGDATE_INDEX = 1;
    private final static int LOGATTRS_INDEX = 4;
    private final static String SUCCESSFUL = "successful";
    private final static String USER = "user";
    private final static String ALLOWED = "allowed";
    private final static Pattern loggerPattern = Pattern.compile("^([\\d\\s\\-:,]+)\\s+(\\w+)\\s+(.*):\\s+(.*)");

    public HDFSSecurityLogObject parse(String logLine) throws ParseException {
        Matcher logMatcher = loggerPattern.matcher(logLine);
        if(!logMatcher.find()) return null;

        HDFSSecurityLogObject ret = new HDFSSecurityLogObject();
        String date = logMatcher.group(LOGDATE_INDEX);
        ret.timestamp = DateTimeUtil.humanDateToMilliseconds(logMatcher.group(LOGDATE_INDEX));

        Map<String, String> attrMap = parseAttr(logMatcher.group(LOGATTRS_INDEX));

        if(attrMap == null) return null;

        ret.user = attrMap.get(USER);
        if(attrMap.get(ALLOWED).equals(ALLOWED)) {
            ret.allowed = true;
        } else {
            ret.allowed = false;
        }

        return ret;
    }

    private Map<String, String> parseAttr(String logLine) {
        if(logLine == null) return null;

        Map<String, String> ret = new HashMap<>();
        String [] fields = logLine.split("\\s+");

        if(fields[1].equalsIgnoreCase(SUCCESSFUL)) {
            ret.put(ALLOWED, "allowed");
        } else {
            ret.put(ALLOWED, "denied");
        }

        String user = LogParseUtil.parseUserFromUGI(fields[3]);
        ret.put(USER, user);
        return ret;
    }
}
