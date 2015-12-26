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

import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class HDFSResourceAccessConfigTest {

	@Test
	public void testHDFSResourceAccessConfig() throws Exception {
		String configStr = "{\"fs.defaultFS\":\"hdfs://sandbox-nn-ha\",\"dfs.nameservices\":\"sandbox-nn-ha\",\"dfs.ha.namenodes.sandbox-nn-ha\":\"nn1,nn2\",\"dfs.namenode.rpc-address.sandbox-nn-ha.nn1\":\"sandbox-nn.vip.ebay.com:8020\",\"dfs.namenode.rpc-address.sandbox-nn-ha.nn2\":\"sandbox-nn-2.vip.ebay.com:8020\",\"dfs.client.failover.proxy.provider.sandbox-nn-ha\":\"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\"hadoop.security.authentication\":\"kerberos\",\"dfs.namenode.kerberos.principal\":\"hadoop/_HOST@EXAMPLE.COM\"}";
		ObjectMapper mapper = new ObjectMapper();
		Map<String, String> configMap = mapper.readValue(configStr, Map.class);
		Map<String, String> result = new HashMap<>();
		result.put("fs.defaultFS" , "hdfs://sandbox-nn-ha");
		result.put("dfs.nameservices", "sandbox-nn-ha");
		result.put("dfs.ha.namenodes.sandbox-nn-ha", "nn1,nn2");
		result.put("dfs.namenode.rpc-address.sandbox-nn-ha.nn1", "sandbox-nn.vip.ebay.com:8020");
		result.put("dfs.namenode.rpc-address.sandbox-nn-ha.nn2", "sandbox-nn-2.vip.ebay.com:8020");
		result.put("dfs.client.failover.proxy.provider.sandbox-nn-ha","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		result.put("hadoop.security.authentication", "kerberos");
		result.put("dfs.namenode.kerberos.principal", "hadoop/_HOST@EXAMPLE.COM");

		Assert.assertEquals(configMap, result);
	}

}
