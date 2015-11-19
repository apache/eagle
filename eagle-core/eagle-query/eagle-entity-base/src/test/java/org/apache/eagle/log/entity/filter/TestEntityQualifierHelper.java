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
package org.apache.eagle.log.entity.filter;

import org.apache.eagle.log.entity.EntityQualifierUtils;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestLogAPIEntity;
import junit.framework.Assert;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
* @since : 10/15/14 2014
*/
public class TestEntityQualifierHelper {
	private EntityDefinition ed;
	@Before
	public void setUp(){
		try {
			if(EntityDefinitionManager.getEntityByServiceName("TestLogAPIEntity") == null){
				EntityDefinitionManager.registerEntity(TestLogAPIEntity.class);
			}
			ed = EntityDefinitionManager.getEntityByServiceName("TestLogAPIEntity");
		} catch (InstantiationException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		} catch (IllegalAccessException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}
	}

	@Test
	public void testEd(){
		Assert.assertNotNull(ed);
		Assert.assertNotNull(ed.getQualifierNameMap().get("a"));
		Assert.assertNull(ed.getQualifierNameMap().get("notexist"));
	}

	@Test
	public void  testIntEntityQualifierHelper(){
		byte[] value = EntityQualifierUtils.toBytes(ed, "field1", "2");
		Assert.assertTrue(Bytes.compareTo(value, Bytes.toBytes(1)) > 0);
		Assert.assertTrue(Bytes.compareTo(value, Bytes.toBytes(2)) == 0);
		Assert.assertTrue(Bytes.compareTo(value, Bytes.toBytes(3)) < 0);
	}

	@Test
	public void  testStringEntityQualifierHelper(){
		byte[] value = EntityQualifierUtils.toBytes(ed, "field7", "xyz");
		Assert.assertTrue(Bytes.compareTo(value, Bytes.toBytes("xyy")) > 0);
		Assert.assertTrue(Bytes.compareTo(value, Bytes.toBytes("xyz")) == 0);
		Assert.assertTrue(Bytes.compareTo(value, Bytes.toBytes("xzz")) < 0);

		Assert.assertTrue(Bytes.compareTo(value, Bytes.toBytes("xy")) > 0);
	}

	@Test
	public void  testDoubleEntityQualifierHelper(){
		byte[] value = EntityQualifierUtils.toBytes(ed, "field5", "1.0");
		Assert.assertTrue(Bytes.compareTo(value,Bytes.toBytes(0.5)) > 0);
		Assert.assertTrue(Bytes.compareTo(value, Bytes.toBytes(1.0)) == 0);
		Assert.assertTrue(Bytes.compareTo(value, Bytes.toBytes(2.2)) < 0);

//      TODO There is problem with negative double
//		Assert.assertTrue(Bytes.compareTo(Bytes.toBytes(-0.6),Bytes.toBytes(-0.5)) < 0);
	}

	@Test
	public void  testLongEntityQualifierHelper(){
		byte[] value = EntityQualifierUtils.toBytes(ed, "field4", "100000");
		Assert.assertTrue(Bytes.compareTo(value,Bytes.toBytes(100000l-1l )) > 0);
		Assert.assertTrue(Bytes.compareTo(value, Bytes.toBytes(100000l)) == 0);
		Assert.assertTrue(Bytes.compareTo(value, Bytes.toBytes(100000l + 1l)) < 0);
	}

	@Test
	public void  testNegativeLongEntityQualifierHelper(){
		Exception ex = null;
		try{
			byte[] value = EntityQualifierUtils.toBytes(ed, "field4", "-100000");
		}catch (IllegalArgumentException e){
			ex = e;
		}
		Assert.assertNull(ex);
	}

	@Test
	public void testParseAsList(){
		List<String> set = EntityQualifierUtils.parseList("(\"abc1\",\"abc2\")");
		Assert.assertEquals(2,set.size());
		Assert.assertEquals("abc1",set.toArray()[0]);
		Assert.assertEquals("abc2",set.toArray()[1]);

		set = EntityQualifierUtils.parseList("(1,\"abc2\")");
		Assert.assertEquals(2,set.size());
		Assert.assertEquals("1",set.toArray()[0]);
		Assert.assertEquals("abc2",set.toArray()[1]);

		set = EntityQualifierUtils.parseList("(-1.5,\"abc2\")");
		Assert.assertEquals(2,set.size());
		Assert.assertEquals("-1.5",set.toArray()[0]);
		Assert.assertEquals("abc2",set.toArray()[1]);

		set = EntityQualifierUtils.parseList("(-1.5,\"-1.5,abc\")");
		Assert.assertEquals(2,set.size());
		Assert.assertEquals("-1.5",set.toArray()[0]);
		Assert.assertEquals("-1.5,abc",set.toArray()[1]);

		set = EntityQualifierUtils.parseList("(-1.5,\"\\\"abc\\\"\")");
		Assert.assertEquals(2,set.size());
		Assert.assertEquals("-1.5",set.toArray()[0]);
		Assert.assertEquals("\"abc\"",set.toArray()[1]);

		set = EntityQualifierUtils.parseList("(-1.5,\"-1.5,\\\"abc\")");
		Assert.assertEquals(2,set.size());
		Assert.assertEquals("-1.5",set.toArray()[0]);
		Assert.assertEquals("-1.5,\"abc",set.toArray()[1]);

		set = EntityQualifierUtils.parseList("(\"\\\"-1.5\\\",abc1\",\"-1.5,\\\"abc2\")");
		Assert.assertEquals(2,set.size());
		Assert.assertEquals("\"-1.5\",abc1",set.toArray()[0]);
		Assert.assertEquals("-1.5,\"abc2",set.toArray()[1]);

		set = EntityQualifierUtils.parseList("(-1.5,\"-1.5,\"abc\")");
		Assert.assertEquals(2,set.size());
		Assert.assertEquals("-1.5",set.toArray()[0]);
		Assert.assertEquals("-1.5,\"abc",set.toArray()[1]);

		set = EntityQualifierUtils.parseList("(\"\\\"value1,part1\\\",\\\"value1,part2\\\"\",\"value2\")");
		Assert.assertEquals(2,set.size());
		Assert.assertEquals("\"value1,part1\",\"value1,part2\"",set.toArray()[0]);
		Assert.assertEquals("value2",set.toArray()[1]);

		////////////////////////////////
		// Bad Format
		////////////////////////////////
		set = EntityQualifierUtils.parseList("(\"a,b)");
		Assert.assertEquals(1,set.size());
		Assert.assertEquals("a,b",set.toArray()[0]);

		set = EntityQualifierUtils.parseList("(a,b\")");
		Assert.assertEquals(2,set.size());
		Assert.assertEquals("a",set.toArray()[0]);
		Assert.assertEquals("b",set.toArray()[1]);

		set = EntityQualifierUtils.parseList("(a\",b)");
		Assert.assertEquals(1,set.size());
		Assert.assertEquals("a\",b",set.toArray()[0]);

		set = EntityQualifierUtils.parseList("(abc,def)");
		Assert.assertEquals(2,set.size());
		Assert.assertEquals("abc",set.toArray()[0]);
		Assert.assertEquals("def",set.toArray()[1]);

		set = EntityQualifierUtils.parseList("(1.5,def)");
		Assert.assertEquals(2,set.size());
		Assert.assertEquals("1.5",set.toArray()[0]);
		Assert.assertEquals("def",set.toArray()[1]);
	}

//	@Test
//	public void testEscapeRegExp(){
//		Assert.assertEquals("abc\\.def",EntityQualifierHelper.escapeRegExp("abc.def"));
//	}
}