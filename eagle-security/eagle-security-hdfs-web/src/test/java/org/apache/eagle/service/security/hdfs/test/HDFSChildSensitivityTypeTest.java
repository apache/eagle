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
package org.apache.eagle.service.security.hdfs.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.eagle.service.security.hdfs.HDFSResourceSensitivityDataJoiner;
import org.junit.Test;

public class HDFSChildSensitivityTypeTest {

	@Test
	public void testChildSensitivityTest() throws Exception {
		List<String> directoryList = new ArrayList<>();
		directoryList.add("/usr");
		directoryList.add("/usr/data");
		directoryList.add("/usr/custdata");	
		 Map<String, String>  sensitivityMap = new HashMap<String, String>();
		 sensitivityMap.put("/usr/data", "EMAIL");
		 sensitivityMap.put("/usr/data/lib", "EMAIL");
		 sensitivityMap.put("/usr/custdata", "PHONE_NUMBER");
		 HDFSResourceSensitivityDataJoiner joiner  = new HDFSResourceSensitivityDataJoiner();
		// HDFSFileSystem fileSystem = new HDFSFileSystem("hdfs://127.0.0.1:9000");
		// joiner.joinFileSensitivity("cluster1-dc1", fileSystem.browse("/usr"));
		 Set<String> types = joiner.getChildSensitivityTypes("/usr/data/", sensitivityMap);
		 System.out.println(types);
		
		///apps/hive/warehouse/xademo.db/customer_details
		
	}
}
