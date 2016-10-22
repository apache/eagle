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
package org.apache.eagle.security.hdfs;

import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.security.util.LogParseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * e.g. 2015-09-21 21:36:52,172 INFO FSNamesystem.audit: allowed=true   ugi=hadoop (auth:KERBEROS)     ip=/x.x.x.x   cmd=getfileinfo src=/tmp   dst=null        perm=null       proto=rpc
 */

public final class HDFSAuditLogParser implements Serializable {
    private final static Logger LOG = LoggerFactory.getLogger(HDFSAuditLogParser.class);

    public HDFSAuditLogParser() {
    }

    public static String parseUser(String ugi) {
        /** e.g.
         * .1)user@APD.xyz.com
         * .2)hadoop/123.dc1.xyz.com@xyz.com (auth:KERBEROS)
         * .3)hadoop (auth:KERBEROS)
         */
        int index = ugi.indexOf("/");
        if (index != -1) {
            return ugi.substring(0, index).trim();
        }
        index = ugi.indexOf("@");
        if (index != -1) {
            return ugi.substring(0, index).trim();
        }
        index = ugi.indexOf("(");
        return ugi.substring(0, index).trim();
    }

    public HDFSAuditLogObject parse(String log) throws Exception {
        int index0 = log.indexOf(" ");
        index0 = log.indexOf(" ", index0 + 1);
        String data = log.substring(0, index0).trim();
        int index1 = log.indexOf("allowed=");
        int len1 = 8;
        int index2 = log.indexOf("ugi=");
        int len2 = 4;
        int index3 = log.indexOf("ip=/");
        int len3 = 4;
        int index4 = log.indexOf("cmd=");
        int len4 = 4;
        int index5 = log.indexOf("src=");
        int len5 = 4;
        int index6 = log.indexOf("dst=");
        int len6 = 4;
        int index7 = log.indexOf("perm=");

        String allowed = log.substring(index1 + len1, index2).trim();
        String ugi = log.substring(index2 + len2, index3).trim();
        String ip = log.substring(index3 + len3, index4).trim();
        String cmd = log.substring(index4 + len4, index5).trim();
        String src = log.substring(index5 + len5, index6).trim();
        String dst = log.substring(index6 + len6, index7).trim();

        HDFSAuditLogObject entity = new HDFSAuditLogObject();
        String user = LogParseUtil.parseUserFromUGI(ugi);
        if (src != null && src.equals("null")) {
            src = null;
        }

        if (dst != null && dst.equals("null")) {
            dst = null;
        }
        entity.user = user;
        entity.cmd = cmd;
        entity.src = src;
        entity.dst = dst;
        entity.host = ip;
        entity.allowed = Boolean.valueOf(allowed);
        entity.timestamp = DateTimeUtil.humanDateToMilliseconds(data);
        return entity;
    }
}