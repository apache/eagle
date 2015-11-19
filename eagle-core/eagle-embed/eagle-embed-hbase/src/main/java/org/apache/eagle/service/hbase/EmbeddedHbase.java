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
package org.apache.eagle.service.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class EmbeddedHbase {
    private HBaseTestingUtility util;
    private MiniHBaseCluster hBaseCluster;
    private static EmbeddedHbase hbase;
    private int port;    
    private String znode;
    private static int DEFAULT_PORT = 2181;
    private static String DEFAULT_ZNODE = "/hbase-unsecure";
	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedHbase.class);
	
    private EmbeddedHbase(int port, String znode) {
    	this.port = port;
    	this.znode = znode;    	
    }
    
    private EmbeddedHbase(int port) {
    	this(port, DEFAULT_ZNODE);
    }
    
    public static EmbeddedHbase getInstance() {
    	if (hbase == null) {
    		synchronized(EmbeddedHbase.class) {
    			if (hbase == null) {
    				hbase = new EmbeddedHbase();
    				hbase.start();   						
    			}
    		}
    	}
    	return hbase;
    }
    
    private EmbeddedHbase() {
    	this(DEFAULT_PORT, DEFAULT_ZNODE);
    }

    public void start() {
    	try {
	    	util = new HBaseTestingUtility();
	        Configuration conf= util.getConfiguration();
	        conf.setInt("test.hbase.zookeeper.property.clientPort", port);
	        conf.set("zookeeper.znode.parent", znode);
	        conf.setInt("hbase.zookeeper.property.maxClientCnxns", 200);
	        conf.setInt("hbase.master.info.port", -1);//avoid port clobbering
	        // start mini hbase cluster
	        hBaseCluster = util.startMiniCluster();
	        Configuration config = hBaseCluster.getConf();
	        
	        config.set("zookeeper.session.timeout", "120000");
	        config.set("hbase.zookeeper.property.tickTime", "6000");
	        config.set(HConstants.HBASE_CLIENT_PAUSE, "3000");
	        config.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "1");
	        config.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "60000");
	        
	        Runtime.getRuntime().addShutdownHook(new Thread() {
	            @Override
	            public void run() {
	            	shutdown();
	            }
	        }); 
    	}
    	catch (Throwable t) {
    		LOG.error("Got an exception: ",t);
    	}
    }

    public void shutdown() {    	
    	try {
            util.shutdownMiniCluster();
        }
    	catch (Throwable t) {
    		LOG.info("Got an exception, " + t , t.getCause());
    		try {
                util.shutdownMiniCluster();
    		}
    		catch (Throwable t1) {
    		}
    	}
    }
    
    public void createTable(String tableName, String cf) {
    	try {    		
    		util.createTable(tableName, cf);
    	}
    	catch (Exception ex) {
    		LOG.warn("Create table failed, probably table already existed, table name: " + tableName);
    	}
    }
    
    public void deleteTable(String tableName){
    	try {
    		util.deleteTable(tableName);
    	}
    	catch (Exception ex) {
    		LOG.warn("Delete table failed, probably table not existed, table name: " + tableName);
    	}
    }

    public static void main(String[] args){
        EmbeddedHbase hbase = new EmbeddedHbase();
        hbase.start();
        for(String table : new Tables().getTables()){
            hbase.createTable(table, "f");
        }
    }
}
