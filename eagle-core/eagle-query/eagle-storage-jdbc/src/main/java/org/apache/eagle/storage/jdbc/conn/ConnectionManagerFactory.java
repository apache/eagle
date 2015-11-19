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
package org.apache.eagle.storage.jdbc.conn;

import org.apache.eagle.storage.jdbc.conn.impl.TorqueConnectionManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 3/27/15
 */
public class ConnectionManagerFactory {
	private final static Logger LOG = LoggerFactory.getLogger(ConnectionManagerFactory.class);
    private static ConnectionManager instance;
    private static volatile boolean initialized; 
    private final static Object lock = new Object();
    
    private static void initialize(ConnectionConfig config) throws Exception{
    	instance = new TorqueConnectionManagerImpl();
        LOG.info("Created new connection manager instance "+instance);
        LOG.info("Initializing connection manager");
        instance.init(config);
        Runtime.getRuntime().addShutdownHook(new ConnectionManagerShutdownHook(instance));
        if(LOG.isDebugEnabled()) LOG.debug("Registered connection manager shutdown hook");
    }

    /**
     * Get instance from eagle configuration
     *
     * @return
     */
    public static ConnectionManager getInstance() throws Exception {
    	if(!initialized){
    		synchronized(lock){
    			if(!initialized){
    				initialize(ConnectionConfigFactory.getFromEagleConfig());
    				initialized = true;
    			}
    		}
    	}
    	return instance;
    }

    private static class ConnectionManagerShutdownHook extends Thread{
        private final ConnectionManager connectionManager;

        ConnectionManagerShutdownHook(ConnectionManager connectionManager){
            this.connectionManager = connectionManager;
        }

        @Override
        public void run() {
            LOG.info("Shutting down");
            try {
                this.connectionManager.shutdown();
            } catch (Exception e) {
                LOG.error(e.getMessage(),e);
            }
        }
    }
}