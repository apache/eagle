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
package org.apache.eagle.alert.coordinator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.eagle.alert.config.ZKConfig;
import org.apache.eagle.alert.config.ZKConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ExclusiveExecutor {

	private static final Logger LOG = LoggerFactory.getLogger(ExclusiveExecutor.class);

	// private static final String PATH = "/alert/listener/leader";
	private static final String COORDINATOR = "coordinator";
	private static final int ZK_RETRYPOLICY_SLEEP_TIME_MS = 1000;
	private static final int ZK_RETRYPOLICY_MAX_RETRIES = 3;

	private static final CuratorFramework client;

	static {
		Config config = ConfigFactory.load().getConfig(COORDINATOR);
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(ZK_RETRYPOLICY_SLEEP_TIME_MS, ZK_RETRYPOLICY_MAX_RETRIES);
		ZKConfig zkConfig = ZKConfigBuilder.getZKConfig(config);
		client = CuratorFrameworkFactory.newClient(zkConfig.zkQuorum, retryPolicy);
		client.start();
	}

	public static abstract class Runnable {

		boolean completed = false;
		LeaderSelector selector;

		public abstract void run() throws Exception;

		public void registerResources(LeaderSelector selector) {
			this.selector = selector;
		}

		public void runElegantly() throws Exception {
			this.run();

			LOG.info("Close selector resources {}", this.selector);
			CloseableUtils.closeQuietly(this.selector);

			completed = true;
		}

		public boolean isCompleted() {
			return completed;
		}

	}

	public static void execute(String path, final Runnable runnable) {
		LeaderSelectorListener listener = new LeaderSelectorListenerAdapter() {

			@Override
			public void takeLeadership(CuratorFramework client) throws Exception {
				// this callback will get called when you are the leader
				// do whatever leader work you need to and only exit
				// this method when you want to relinquish leadership
				LOG.info("this is leader node right now..");
				runnable.runElegantly();
			}

			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				LOG.info(String.format("leader selector state change listener, new state: %s", newState.toString()));
			}

		};

		LeaderSelector selector = new LeaderSelector(client, path, listener);
		selector.autoRequeue(); // not required, but this is behavior that you
								// will probably expect
		selector.start();

		runnable.registerResources(selector);

		Runtime.getRuntime().addShutdownHook(new Thread(new java.lang.Runnable() {

			@Override
			public void run() {
				LOG.info("Close zk client resources {}", ExclusiveExecutor.client);
				CloseableUtils.closeQuietly(ExclusiveExecutor.client);
			}

		}));
	}

}
