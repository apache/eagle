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
package org.apache.eagle.security.auditlog;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.eagle.security.hdfs.HDFSAuditLogObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.dataproc.impl.storm.kafka.SpoutKafkaMessageDeserializer;
import org.apache.eagle.security.hdfs.HDFSAuditLogParser;
import org.apache.eagle.security.hdfs.HDFSAuditLogObject;

public class HdfsAuditLogKafkaDeserializer implements SpoutKafkaMessageDeserializer{
	private static Logger LOG = LoggerFactory.getLogger(HdfsAuditLogKafkaDeserializer.class);
	private Properties props;

	public  HdfsAuditLogKafkaDeserializer(Properties props){
		this.props = props;
	}
	
	/**
	 * the steps for deserializing message from kafka
	 * 1. convert byte array to string
	 * 2. parse string to eagle entity
	 */
	@Override
	public Object deserialize(byte[] arg0) {
		String logLine = new String(arg0);

		HDFSAuditLogParser parser = new HDFSAuditLogParser();
		HDFSAuditLogObject entity = null;
		try{
			entity = parser.parse(logLine);
		}catch(Exception ex){
			LOG.error("Failing parse audit log message", ex);
		}
		if(entity == null){
			LOG.warn("Event ignored as it can't be correctly parsed, the log is ", logLine);
			return null;
		}
		Map<String, Object> map = new TreeMap<String, Object>();
		map.put("src", entity.src);
		map.put("dst", entity.dst);
		map.put("host", entity.host);
		map.put("timestamp", entity.timestamp);
		map.put("allowed", entity.allowed);
		map.put("user", entity.user);
		map.put("cmd", entity.cmd);
		
		return map;
	}
}
