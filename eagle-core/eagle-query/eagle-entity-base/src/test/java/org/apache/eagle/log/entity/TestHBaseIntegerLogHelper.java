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
package org.apache.eagle.log.entity;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.common.ByteUtil;
import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @since : 11/10/14,2014
 */
public class TestHBaseIntegerLogHelper {
	@Test
	public void testTimeSeriesAPIEntity(){
		InternalLog internalLog = new InternalLog();
		Map<String,byte[]> map = new HashMap<String,byte[]>();
		TestTimeSeriesAPIEntity apiEntity = new TestTimeSeriesAPIEntity();
		EntityDefinition ed = null;
		try {
			ed = EntityDefinitionManager.getEntityByServiceName("TestTimeSeriesAPIEntity");
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		map.put("a", ByteUtil.intToBytes(12));
		map.put("c", ByteUtil.longToBytes(123432432l));
		map.put("cluster", new String("cluster4ut").getBytes());
		map.put("datacenter", new String("datacenter4ut").getBytes());

		internalLog.setQualifierValues(map);
		internalLog.setTimestamp(System.currentTimeMillis());

		try {
			TaggedLogAPIEntity entity = HBaseInternalLogHelper.buildEntity(internalLog, ed);
			Assert.assertTrue(entity instanceof TestTimeSeriesAPIEntity);
			TestTimeSeriesAPIEntity tsentity = (TestTimeSeriesAPIEntity) entity;
			Assert.assertEquals("cluster4ut",tsentity.getTags().get("cluster"));
			Assert.assertEquals("datacenter4ut",tsentity.getTags().get("datacenter"));
			Assert.assertEquals(12,tsentity.getField1());
			Assert.assertEquals(123432432l,tsentity.getField3());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
