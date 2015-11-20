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
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class HDFSAuditLogParser implements Serializable{
	private final static Logger LOG = LoggerFactory.getLogger(HDFSAuditLogParser.class);

	private final static int LOGDATE_INDEX = 1;
	private final static int LOGLEVEL_INDEX = 2;
	private final static int LOGGER_INDEX = 3;
	private final static int LOGATTRS_INDEX = 4;
	private final static String LOGDATE="logdate";
	private final static String LOGLEVEL="loglevel";
	private final static String LOGHEADER="logheader";
	private final static String ALLOWED="allowed";
	private final static String UGI="ugi";
	private final static String IP="ip";
	private final static String CMD="cmd";
	private final static String SRC="src";
	private final static String DST="dst";
	private final static String PERM="perm";
	private final static Pattern loggerPattern = Pattern.compile("^([\\d\\s\\-:,]+)\\s+(\\w+)\\s+(.*):\\s+(.*)");
	private final static Pattern loggerAttributesPattern = Pattern.compile("(\\w+=[/.@\\-\\w\\\\$\\s\\\\(\\\\):]+)\\s+");

	public HDFSAuditLogParser(){
	}

	public HDFSAuditLogObject parse(String logLine) throws Exception{
		Map<String,String> auditMaps = parseAudit(logLine);
		if(auditMaps == null) return null;
        HDFSAuditLogObject entity = new HDFSAuditLogObject();

		String ugi = auditMaps.get(UGI);
		if(ugi == null){
			LOG.warn("Ugi is null from audit log: " + logLine);
		}

		String user = LogParseUtil.parseUserFromUGI(ugi);

		if(user == null){
			LOG.warn("User is null from ugi" + ugi + ", audit log: " + logLine);
		}

		String src = auditMaps.get(SRC);
		if(src != null && src.equals("null")){
			src = null;
		}

		String dst = auditMaps.get(DST);
		if(dst != null && dst.equals("null")){
			dst = null;
		}


		String cmd = auditMaps.get(CMD);
		if(cmd == null){
			LOG.warn("Cmd is null from audit log: " + logLine);
		}

        entity.user = user;
        entity.cmd = cmd;
        entity.src = src;
        entity.dst = dst;
        entity.host = auditMaps.get(IP);
        entity.allowed = Boolean.valueOf(auditMaps.get(ALLOWED));
        entity.timestamp = DateTimeUtil.humanDateToMilliseconds(auditMaps.get(LOGDATE));

		return entity;
	}

	private static Map<String,String> parseAttribute(String attrs){
		Matcher matcher = loggerAttributesPattern.matcher(attrs+" ");
		Map<String,String> attrMap=new HashMap<String, String>();
		while(matcher.find()){
			String kv = matcher.group();
			String[] kvs = kv.split("=");
			if(kvs.length>=2){
				attrMap.put(kvs[0].toLowerCase(),kvs[1].trim());
			}else{
				attrMap.put(kvs[0].toLowerCase(),null);
			}
		}
		return attrMap;
	}

	private Map<String,String> parseAudit(String log){
		Matcher loggerMatcher = loggerPattern.matcher(log);
		Map<String,String> map = null;
		if(loggerMatcher.find()){
			try{
				map = new HashMap<String, String>();
				map.put(LOGDATE, loggerMatcher.group(LOGDATE_INDEX)); // logdate
				map.put(LOGLEVEL, loggerMatcher.group(LOGLEVEL_INDEX)); // level
				map.put(LOGHEADER, loggerMatcher.group(LOGGER_INDEX)); // logg
				Map<String,String> loggerAttributes = parseAttribute(loggerMatcher.group(LOGATTRS_INDEX));
				map.put(ALLOWED, loggerAttributes.get(ALLOWED));
				map.put(UGI, loggerAttributes.get(UGI));
				map.put(IP, loggerAttributes.get(IP));
				map.put(CMD, loggerAttributes.get(CMD));
				map.put(SRC, loggerAttributes.get(SRC));
				map.put(DST, loggerAttributes.get(DST));
				map.put(PERM, loggerAttributes.get(PERM));
			}catch (IndexOutOfBoundsException e){
				LOG.error("Got exception when parsing audit log:" + log + ", exception:" + e.getMessage(), e);
				map = null;
			}
		}
		return map;
	}
}
