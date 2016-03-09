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
package org.apache.eagle.service.security.hdfs.rest;

import org.apache.eagle.service.security.hdfs.HDFSResourceUtils;

/**
 * Validate the REST API request
 */
public class HDFSResourceWebRequestValidator {

	/**
	 * validate HDFSResource Site and File Path
	 * @param site
	 * @param filePath
	 * @throws Exception
	 */
	public void validate( String site, String filePath ) throws Exception {
		if (HDFSResourceUtils.isNullOrEmpty(site))
			throw new Exception("Invalid Request Received ... Site is Empty Or Null..");
		if (HDFSResourceUtils.isNullOrEmpty(filePath))
			throw new Exception("Invalid Request Received ... file/Directory Path is Empty Or Null..");
	}
}