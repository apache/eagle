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
package org.apache.eagle.alert.config;

import org.junit.Assert;
import org.junit.Test;

import org.apache.eagle.dataproc.core.JsonSerDeserUtils;

public class TestAlertDedup {

	@Test
	public void test() throws Exception{
		String alertDef = "{\"alertDedupIntervalMin\":\"720\",\"emailDedupIntervalMin\":\"1440\"}";
		DeduplicatorConfig dedupConfig = JsonSerDeserUtils.deserialize(alertDef, DeduplicatorConfig.class);
		Assert.assertEquals(dedupConfig.getAlertDedupIntervalMin(), 720);
		Assert.assertEquals(dedupConfig.getEmailDedupIntervalMin(), 1440);
		
		alertDef = "null";
		dedupConfig = JsonSerDeserUtils.deserialize(alertDef, DeduplicatorConfig.class);
		Assert.assertEquals(dedupConfig, null);
	}

}
