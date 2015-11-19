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

package org.apache.eagle.security.util;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParseUtil {
    /**
     * .e.g. user@APD.xyz.com
     */
    private final static Pattern UGI_PATTERN_DEFAULT = Pattern.compile("^([\\w\\d\\-]+)@.*");
    /**
     * .e.g. hadoop/123.dc1.xyz.com@xyz.com (auth:KERBEROS)
     */
    private final static Pattern UGI_PATTERN_SYSTEM = Pattern.compile("^([\\w\\d\\-]+)/[\\w\\d\\-.]+@.*");

    /**
     * .e.g. hadoop (auth:KERBEROS)
     */
    private final static Pattern UGI_PATTERN_WITHBLANK = Pattern.compile("^([\\w\\d.\\-_]+)[\\s(]+.*");

    /**
     * @param ugi UGI field of audit log
     * @return resultToMetrics user from UGI field of audit log
     */
    public static  String parseUserFromUGI(String ugi) {
        if(ugi == null) return null;
        String newugi = ugi.trim();

        Matcher defaultMatcher = UGI_PATTERN_DEFAULT.matcher(newugi);
        if(defaultMatcher.matches()){
            return defaultMatcher.group(1);
        }

        Matcher sysMatcher = UGI_PATTERN_SYSTEM.matcher(newugi);
        if(sysMatcher.matches()){
            return sysMatcher.group(1);
        }

        Matcher viaMatcher = UGI_PATTERN_WITHBLANK.matcher(newugi);
        if(viaMatcher.matches()){
            return viaMatcher.group(1);
        }

        return newugi;
    }
}
