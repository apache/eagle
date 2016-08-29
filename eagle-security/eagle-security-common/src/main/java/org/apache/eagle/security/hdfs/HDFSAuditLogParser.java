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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * e.g. 2015-09-21 21:36:52,172 INFO FSNamesystem.audit: allowed=true   ugi=hadoop (auth:KERBEROS)     ip=/x.x.x.x   cmd=getfileinfo src=/tmp   dst=null        perm=null       proto=rpc
 */

public final class HDFSAuditLogParser implements Serializable{
	private final static Logger LOG = LoggerFactory.getLogger(HDFSAuditLogParser.class);
	private final static Pattern loggerPattern = Pattern.compile("(.+)\\s+INFO.+allowed=(.*)\\s+ugi=(.*)\\s+ip=/(.*)\\s+cmd=(.*)\\s+src=(.*)\\s+dst=(.*)\\s+perm=(.*)\\s+proto=(.*)");

	public HDFSAuditLogParser(){}

	public HDFSAuditLogObject parse(String log) throws Exception{

		Matcher loggerMatcher = loggerPattern.matcher(log);

		if(!loggerMatcher.find())
			LOG.warn("Regex matching failed for HDFS audit log: " + log);

		String date = loggerMatcher.group(1).trim();
		String allowed = loggerMatcher.group(2).trim();
		String ugi = loggerMatcher.group(3).trim();
		String ip = loggerMatcher.group(4).trim();
		String cmd = loggerMatcher.group(5).trim();
		String src = loggerMatcher.group(6).trim();
		String dst = loggerMatcher.group(7).trim();
		String perm = loggerMatcher.group(8).trim();
		String proto = loggerMatcher.group(9).trim();

		String user = LogParseUtil.parseUserFromUGI(ugi);
		if (src != null && src.equals("null")) {
			src = null;
		}

		if (dst != null && dst.equals("null")) {
			dst = null;
		}

		HDFSAuditLogObject entity = new HDFSAuditLogObject();
		entity.user = user;
		entity.cmd = cmd;
		entity.src = src;
		entity.dst = dst;
		entity.host = ip;
		entity.allowed = Boolean.valueOf(allowed);
		entity.timestamp = DateTimeUtil.humanDateToMilliseconds(date);
		return entity;
	}
}