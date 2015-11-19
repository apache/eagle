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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;

public class TestHbaseWritePerformance {

	public static void main(String[] args) throws IOException {
		
		HTableInterface tbl = EagleConfigFactory.load().getHTable("unittest");

		int putSize = 1000;
		List<Put> list = new ArrayList<Put>(putSize);
		for (int i = 0; i < putSize; ++i) {
			byte[] v = Integer.toString(i).getBytes();
			Put p = new Put(v);
			p.add("f".getBytes(), "a".getBytes(), 100, v);
			list.add(p);
		}

		// Case 1
		System.out.println("Case 1: autoflush = true, individual put");
		tbl.setAutoFlush(true);
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < 1; ++i) {
			for (Put p : list) {
				tbl.put(p);
			}
			tbl.flushCommits();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("Case 1: " + (endTime - startTime) + " ms");
		
		
		// Case 2
		System.out.println("Case 2: autoflush = true, multi-put");
		tbl.setAutoFlush(true);
		startTime = System.currentTimeMillis();
		for (int i = 0; i < 1; ++i) {
			tbl.put(list);
			tbl.flushCommits();
		}
		endTime = System.currentTimeMillis();
		System.out.println("Case 2: " + (endTime - startTime) + " ms");

		
		// Case 3
		System.out.println("Case 3: autoflush = false, multi-put");
		tbl.setAutoFlush(false);
		startTime = System.currentTimeMillis();
		for (int i = 0; i < 1; ++i) {
			tbl.put(list);
			tbl.flushCommits();
		}
		endTime = System.currentTimeMillis();
		System.out.println("Case 3: " + (endTime - startTime) + " ms");

		
		// Case 4
		System.out.println("Case 4: autoflush = false, individual put");
		tbl.setAutoFlush(true);
		startTime = System.currentTimeMillis();
		for (int i = 0; i < 1; ++i) {
			for (Put p : list) {
				tbl.put(p);
			}
			tbl.flushCommits();
		}
		endTime = System.currentTimeMillis();
		System.out.println("Case 4: " + (endTime - startTime) + " ms");

	}
	
	@Test
	public void test() {
		
	}
}
