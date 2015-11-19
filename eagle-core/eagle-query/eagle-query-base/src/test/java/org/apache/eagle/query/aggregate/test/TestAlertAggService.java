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
package org.apache.eagle.query.aggregate.test;

import java.util.ArrayList;
import java.util.List;


import org.junit.Test;

public class TestAlertAggService {
	@Test
	public void testCompileAndSplitCondition(){
		List<String> alertTagNameValues = new ArrayList<String>();
		String tagNameValue1 = "cluster=cluster1";
		String tagNameValue2 = "category=checkHadoopFS";
		String tagNameValue3 = "category=highloadDisk";
		String tagNameValue4 = "cluster=dc124";
		String tagNameValue5 = "category=lowloadDisk";
		alertTagNameValues.add(tagNameValue1);
		alertTagNameValues.add(tagNameValue2);
		alertTagNameValues.add(tagNameValue3);
		alertTagNameValues.add(tagNameValue4);
		alertTagNameValues.add(tagNameValue5);
//		AlertAggResource r = new AlertAggResource();
//		List<List<String>> result = r.compileAndSplitConditions(alertTagNameValues);
//		Assert.assertEquals(result.size(), 3);
//		Assert.assertEquals(result.get(0).size(), 3);
//		Assert.assertTrue(result.get(0).contains(tagNameValue2));
//		Assert.assertTrue(result.get(0).contains(tagNameValue1));
//		Assert.assertTrue(result.get(0).contains(tagNameValue4));
//		Assert.assertEquals(result.get(1).size(), 3);
//		Assert.assertTrue(result.get(1).contains(tagNameValue3));
//		Assert.assertTrue(result.get(1).contains(tagNameValue1));
//		Assert.assertTrue(result.get(1).contains(tagNameValue4));
//		Assert.assertEquals(result.get(2).size(), 3);
//		Assert.assertTrue(result.get(2).contains(tagNameValue5));
//		Assert.assertTrue(result.get(2).contains(tagNameValue1));
//		Assert.assertTrue(result.get(2).contains(tagNameValue4));
	}
}

