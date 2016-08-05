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

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

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

/**
 * @since Jun 16, 2016
 *
 */
public class CoordinatorListener implements ServletContextListener {
    
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorListener.class);
    
    private static final String PATH = "/alertleader";
    private static final String COORDINATOR = "coordinator";
    
    private final Config config;
    
    private LeaderSelector selector;
    private CuratorFramework client;
    
    public CoordinatorListener() {
    	config = ConfigFactory.load().getConfig(COORDINATOR);
    }
    
    @Override
    public void contextInitialized(ServletContextEvent sce) {
        LeaderSelectorListener listener = new LeaderSelectorListenerAdapter() {
        	
        	@Override
            public void takeLeadership(CuratorFramework client) throws Exception {
                // this callback will get called when you are the leader
                // do whatever leader work you need to and only exit
                // this method when you want to relinquish leadership
            	LOG.info("this is leader node right now..");
            	LOG.info("start coordinator background tasks..");
                Coordinator.startSchedule();
                
                Thread.currentThread().join();
            }
            
        	@Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
            	LOG.info(String.format("leader selector listener, new state: %s", newState.toString()));
            }

        };
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        ZKConfig zkConfig = ZKConfigBuilder.getZKConfig(config);
        client = CuratorFrameworkFactory.newClient(zkConfig.zkQuorum, retryPolicy);
        client.start();
        
        selector = new LeaderSelector(client, PATH, listener);
        selector.autoRequeue();  // not required, but this is behavior that you will probably expect
        selector.start();
    }
    
    @Override
    public void contextDestroyed(ServletContextEvent sce) {
    	CloseableUtils.closeQuietly(selector);
    	CloseableUtils.closeQuietly(client);
    }

}
