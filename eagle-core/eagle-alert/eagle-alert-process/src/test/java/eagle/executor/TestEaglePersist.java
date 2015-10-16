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
/**
 * 
 */
package eagle.executor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import eagle.alert.persist.EaglePersist;
import eagle.alert.base.AlertTestBase;
import eagle.alert.entity.AlertAPIEntity;

/**
 * @since Mar 20, 2015
 */
public class TestEaglePersist extends AlertTestBase{

	public AlertAPIEntity mokeupEntity() {
		AlertAPIEntity entity = new AlertAPIEntity();
		Map<String, String> tags = new HashMap<String, String>();
		entity.setTags(tags);
		entity.getTags().put("category", "test_category");
		return entity;
	}
	
	@Test
	public void test() throws Exception {
		hbase.createTable("alertdetail", "f");
		EaglePersist persist = new EaglePersist("localhost", 8080);
		boolean isSuccess = persist.doPersist(Arrays.asList(mokeupEntity()));
		Assert.assertTrue(isSuccess == true);
	}
}
