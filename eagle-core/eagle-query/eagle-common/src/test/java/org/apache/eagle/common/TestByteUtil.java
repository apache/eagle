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
package org.apache.eagle.common;

import junit.framework.Assert;

import org.junit.Test;

public class TestByteUtil {
	
	@Test
	public void testLongAndBytesConversion() {
		long origValue = 0x1234567812345678L;
		byte[] bytes = ByteUtil.longToBytes(origValue);
		checkNonZeros(bytes);
		long value = ByteUtil.bytesToLong(bytes);
		Assert.assertEquals(origValue, value);
		bytes = new byte[16];
		checkZeros(bytes);
		ByteUtil.longToBytes(origValue, bytes, 4);
		checkZeros(bytes, 0, 4);
		checkZeros(bytes, 12, 16);
		checkNonZeros(bytes, 4, 12);
		value = ByteUtil.bytesToLong(bytes, 4);
		Assert.assertEquals(origValue, value);
	}
	
	@Test
	public void testDoubleAndBytesConversion() {
		double origValue =  (double)0x1234567812345678L;
		byte[] bytes = ByteUtil.doubleToBytes(origValue);
		checkNonZeros(bytes);
		double value = ByteUtil.bytesToDouble(bytes);
		Assert.assertEquals(origValue, value);
		bytes = new byte[16];
		checkZeros(bytes);
		ByteUtil.doubleToBytes(origValue, bytes, 4);
		checkZeros(bytes, 0, 4);
		checkZeros(bytes, 12, 16);
		checkNonZeros(bytes, 4, 12);
		value = ByteUtil.bytesToDouble(bytes, 4);
		Assert.assertEquals(origValue, value);
	}
	
	@Test
	public void testIntAndBytesConversion() {
		int origValue = 0x12345678;
		byte[] bytes = ByteUtil.intToBytes(origValue);
		Assert.assertEquals(4, bytes.length);
		Assert.assertEquals(0x12, bytes[0]);
		Assert.assertEquals(0x34, bytes[1]);
		Assert.assertEquals(0x56, bytes[2]);
		Assert.assertEquals(0x78, bytes[3]);
		checkNonZeros(bytes);
		int value = ByteUtil.bytesToInt(bytes);
		Assert.assertEquals(origValue, value);
		bytes = new byte[12];
		checkZeros(bytes);
		ByteUtil.intToBytes(origValue, bytes, 4);
		checkZeros(bytes, 0, 4);
		checkZeros(bytes, 8, 12);
		checkNonZeros(bytes, 4, 8);
		value = ByteUtil.bytesToInt(bytes, 4);
		Assert.assertEquals(origValue, value);
	}

	@Test
	public void testShortAndBytesConversion() {
		short origValue = 0x1234;
		byte[] bytes = ByteUtil.shortToBytes(origValue);
		Assert.assertEquals(2, bytes.length);
		Assert.assertEquals(0x12, bytes[0]);
		Assert.assertEquals(0x34, bytes[1]);
		checkNonZeros(bytes);
		short value = ByteUtil.bytesToShort(bytes);
		Assert.assertEquals(origValue, value);
	}

	private void checkZeros(byte[] bytes) {
		checkZeros(bytes, 0, bytes.length);
	}
	
	private void checkZeros(byte[] bytes, int i, int j) {
		for (int k = i; k < j; ++k) {
			Assert.assertEquals((byte)0, bytes[k]);
		}
	}

	private void checkNonZeros(byte[] bytes) {
		checkNonZeros(bytes, 0, bytes.length);
	}
	
	private void checkNonZeros(byte[] bytes, int i, int j) {
		for (int k = i; k < j; ++k) {
			Assert.assertNotSame((byte)0, bytes[k]);
		}
	}
}
