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

public class LogParseUtil {

    /**
     * @param ugi UGI field of audit log
     * @return resultToMetrics user from UGI field
     * e.g.
     * 1)user@APD.xyz.com
     * 2)hadoop/123.dc1.xyz.com@xyz.com (auth:KERBEROS)
     * 3)hadoop (auth:KERBEROS)
     * 4)hadoop
     */
    public static String parseUserFromUGI(String ugi) {
        if(ugi == null) return null;
        String newUgi = ugi.trim();
        int index = newUgi.indexOf("/");
        if (index != -1) return newUgi.substring(0, index).trim();
        index = newUgi.indexOf("@");
        if (index != -1) return newUgi.substring(0, index).trim();
        index = newUgi.indexOf("(");
        return newUgi.substring(0, index).trim();
    }
}
