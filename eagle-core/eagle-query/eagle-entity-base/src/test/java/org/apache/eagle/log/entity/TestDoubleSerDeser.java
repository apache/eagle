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

import org.apache.eagle.log.entity.meta.DoubleSerDeser;
import org.apache.eagle.common.ByteUtil;
import junit.framework.Assert;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.junit.Test;

public class TestDoubleSerDeser {

	@Test
	public void test() {
		DoubleSerDeser dsd = new DoubleSerDeser();
		//byte[] t = {'N', 'a', 'N'};
		byte [] t = dsd.serialize(Double.NaN); 
	
		Double d = dsd.deserialize(t);
		System.out.println(d);
		//Double d = dsd.deserialize(t);		
	}

	/**
	 * @link http://en.wikipedia.org/wiki/Double-precision_floating-point_format
	 */
	@Test
	public void testIEEE754_Binary64_DoublePrecisionFloatingPointFormat(){
		for(Double last = null,i=Math.pow(-2.0,33);i< Math.pow(2.0,33);i+=Math.pow(2.0,10)){
			if(last != null){
				Assert.assertTrue(i > last);
				if(last < 0 && i <0){
					Assert.assertTrue("Negative double value and its  serialization Binary array have negative correlation", new BinaryComparator(ByteUtil.doubleToBytes(i)).compareTo(ByteUtil.doubleToBytes(last)) < 0);
				}else if(last < 0 && i >=0){
					Assert.assertTrue("Binary array for negative double is always greater than any positive doubles' ",new BinaryComparator(ByteUtil.doubleToBytes(i)).compareTo(ByteUtil.doubleToBytes(last)) < 0);
				}else if(last >= 0){
					Assert.assertTrue("Positive double value and its  serialization Binary array have positive correlation",new BinaryComparator(ByteUtil.doubleToBytes(i)).compareTo(ByteUtil.doubleToBytes(last)) > 0);
				}
			}
			last = i;
		}
		Assert.assertTrue("Binary array for negative double is always greater than any positive doubles'",new BinaryComparator(ByteUtil.doubleToBytes(-1.0)).compareTo(ByteUtil.doubleToBytes(Math.pow(2.0,32)))>0) ;
	}
}
