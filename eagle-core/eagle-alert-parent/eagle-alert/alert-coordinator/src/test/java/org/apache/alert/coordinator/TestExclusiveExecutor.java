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
package org.apache.alert.coordinator;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.eagle.alert.coordinator.ExclusiveExecutor;
import org.apache.eagle.alert.utils.ZookeeperEmbedded;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Joiner;

@Ignore
public class TestExclusiveExecutor {

	ZookeeperEmbedded zkEmbed;

	@Before
	public void setUp() throws Exception {
		zkEmbed = new ZookeeperEmbedded(2181);
		zkEmbed.start();

		Thread.sleep(2000);
	}

	@After
	public void tearDown() throws Exception {
		zkEmbed.shutdown();
	}

	@Test
	public void testConcurrency() throws Exception {
		ByteArrayOutputStream newStreamOutput = new ByteArrayOutputStream();
		PrintStream newStream = new PrintStream(newStreamOutput);
		PrintStream oldStream = System.out;

		System.setOut(newStream);

		ExclusiveExecutor.Runnable runnableOne = new ExclusiveExecutor.Runnable() {

			@Override
			public void run() throws Exception {
				System.out.println("this is thread one");
			}

		};

		new Thread(new Runnable() {

			@Override
			public void run() {
				ExclusiveExecutor.execute("/alert/test/leader", runnableOne);
			}

		}).start();

		ExclusiveExecutor.Runnable runnableTwo = new ExclusiveExecutor.Runnable() {

			@Override
			public void run() throws Exception {
				System.out.println("this is thread two");
			}

		};
		new Thread(new Runnable() {

			@Override
			public void run() {
				ExclusiveExecutor.execute("/alert/test/leader", runnableTwo);
			}

		}).start();

		Thread.sleep(2000);

		System.out.flush();
		BufferedReader br = new BufferedReader(new StringReader(newStreamOutput.toString()));
		List<String> logs = new ArrayList<String>();
		String line = null;
		while ((line = br.readLine()) != null) {
			logs.add(line);
		}

		System.setOut(oldStream);
		System.out.println("Cached logs: " + Joiner.on("\n").join(logs));

		Assert.assertTrue(logs.stream().anyMatch((log) -> log.contains("this is thread one")));
		Assert.assertTrue(logs.stream().anyMatch((log) -> log.contains("this is thread two")));

		Assert.assertTrue(runnableOne.isCompleted());
		Assert.assertTrue(runnableTwo.isCompleted());
	}

}
