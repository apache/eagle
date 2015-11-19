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
package org.apache.eagle.common.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class EagleConfigFactory implements EagleConfig {
	private static final Logger LOG = LoggerFactory.getLogger(EagleConfigFactory.class);

	private String env;
	private String zkQuorum;
	private String zkPort;

    private Configuration hbaseConf;
	private String eagleServiceHost;
	private int eagleServicePort;
    private String storageType;
    private Config config;
    private TimeZone timeZone;

    public boolean isCoprocessorEnabled() {
		return isCoprocessorEnabled;
	}

	private boolean isCoprocessorEnabled;

	private boolean tableNamePrefixedWithEnv;

	private HTablePool pool;
	private int hbaseClientScanCacheSize = 1000;

	private ThreadPoolExecutor executor = null;

	private static EagleConfigFactory manager = new EagleConfigFactory();

	private EagleConfigFactory(){
		init();
		this.pool = new HTablePool(this.hbaseConf, 10);
	}
	
	public static EagleConfig load(){
		return manager;
	}
	
	public HTableInterface getHTable(String tableName){
        return pool.getTable(tableName);
    }

    private String getString(Config config,String path,String defaultValue){
        if(config.hasPath(path)){
            return config.getString(path);
        }else{
            return defaultValue;
        }
    }

    public String getStorageType() {
        return storageType;
    }

    public ThreadPoolExecutor getExecutor() {
        return executor;
    }

    private void init(){
        this.config = ConfigFactory.load();
        this.timeZone = TimeZone.getTimeZone((config.hasPath(EagleConfigConstants.EAGLE_TIME_ZONE)? config.getString(EagleConfigConstants.EAGLE_TIME_ZONE): EagleConfigConstants.DEFAULT_EAGLE_TIME_ZONE));
        this.env = config.hasPath(EagleConfigConstants.SERVICE_ENV) ? config.getString(EagleConfigConstants.SERVICE_ENV):"dev";
		this.zkQuorum = config.hasPath(EagleConfigConstants.SERVICE_HBASE_ZOOKEEPER_QUORUM) ? config.getString(EagleConfigConstants.SERVICE_HBASE_ZOOKEEPER_QUORUM):null;
		this.zkPort = config.hasPath(EagleConfigConstants.SERVICE_HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT) ? config.getString(EagleConfigConstants.SERVICE_HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT): null;
        String zkZnodeParent = config.hasPath(EagleConfigConstants.SERVICE_ZOOKEEPER_ZNODE_PARENT)? config.getString(EagleConfigConstants.SERVICE_ZOOKEEPER_ZNODE_PARENT): EagleConfigConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT;
		String clientIPCPoolSize = getString(config, EagleConfigConstants.SERVICE_HBASE_CLIENT_IPC_POOL_SIZE, "10");
		this.hbaseConf = HBaseConfiguration.create();

        if (this.zkQuorum != null)
            this.hbaseConf.set("hbase.zookeeper.quorum", this.zkQuorum);

		if (this.zkPort != null)
            this.hbaseConf.set("hbase.zookeeper.property.clientPort", this.zkPort);

        if(zkZnodeParent != null)
            this.hbaseConf.set("zookeeper.znode.parent", zkZnodeParent);
        else
            this.hbaseConf.set("zookeeper.znode.parent", EagleConfigConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);

        this.hbaseConf.set("hbase.client.ipc.pool.size", clientIPCPoolSize);

		this.eagleServiceHost = config.hasPath(EagleConfigConstants.SERVICE_HOST) ? config.getString(EagleConfigConstants.SERVICE_HOST) : EagleConfigConstants.DEFAULT_SERVICE_HOST;
        this.storageType = config.hasPath(EagleConfigConstants.SERVICE_STORAGE_TYPE) ? config.getString(EagleConfigConstants.SERVICE_STORAGE_TYPE) : EagleConfigConstants.DEFAULT_STORAGE_TYPE;
        this.isCoprocessorEnabled = config.hasPath(EagleConfigConstants.SERVICE_COPROCESSOR_ENABLED) && config.getBoolean(EagleConfigConstants.SERVICE_COPROCESSOR_ENABLED);
		this.eagleServicePort = config.hasPath(EagleConfigConstants.SERVICE_PORT) ? config.getInt(EagleConfigConstants.SERVICE_PORT) : EagleConfigConstants.DEFAULT_SERVICE_PORT;
        this.tableNamePrefixedWithEnv = config.hasPath(EagleConfigConstants.SERVICE_TABLE_NAME_PREFIXED_WITH_ENVIRONMENT) && config.getBoolean(EagleConfigConstants.SERVICE_TABLE_NAME_PREFIXED_WITH_ENVIRONMENT);
        this.hbaseClientScanCacheSize = config.hasPath(EagleConfigConstants.SERVICE_HBASE_CLIENT_SCAN_CACHE_SIZE)? config.getInt(EagleConfigConstants.SERVICE_HBASE_CLIENT_SCAN_CACHE_SIZE) : hbaseClientScanCacheSize;
        // initilize eagle service thread pool for parallel execution of hbase scan etc.
		int threadPoolCoreSize = config.hasPath(EagleConfigConstants.SERVICE_THREADPOOL_CORE_SIZE)? config.getInt(EagleConfigConstants.SERVICE_THREADPOOL_CORE_SIZE): EagleConfigConstants.DEFAULT_THREAD_POOL_CORE_SIZE;
		int threadPoolMaxSize = config.hasPath(EagleConfigConstants.SERVICE_THREADPOOL_MAX_SIZE) ? config.getInt(EagleConfigConstants.SERVICE_THREADPOOL_MAX_SIZE) : EagleConfigConstants.DEFAULT_THREAD_POOL_MAX_SIZE;
		long threadPoolShrinkTime = config.hasPath(EagleConfigConstants.SERVICE_THREADPOOL_SHRINK_SIZE) ? config.getLong(EagleConfigConstants.SERVICE_THREADPOOL_SHRINK_SIZE) : EagleConfigConstants.DEFAULT_THREAD_POOL_SHRINK_TIME;

		this.executor = new ThreadPoolExecutor(threadPoolCoreSize, threadPoolMaxSize, threadPoolShrinkTime, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

		LOG.info("Successfully initialized config");

		if(LOG.isDebugEnabled()) {
			if(this.isCoprocessorEnabled){
				LOG.debug("Eagle HBase Coprocessor is enabled");
			}else{
				LOG.debug("Eagle HBase Coprocessor is disabled");
			}
		}
	}

    @Override
	public String getZKQuorum(){
		return this.zkQuorum;
    }

    @Override
	public String getZKPort(){
		return this.zkPort;
	}

    @Override
	public String getServiceHost() {
		return eagleServiceHost;
	}

    @Override
	public int getServicePort() {
		return eagleServicePort;
	}

    @Override
	public String getEnv() {
		return env;
	}

    @Override
	public boolean isTableNamePrefixedWithEnvironment(){
		return this.tableNamePrefixedWithEnv;
	}

    @Override
	public int getHBaseClientScanCacheSize(){
		return this.hbaseClientScanCacheSize;
	}

    @Override
    public TimeZone getTimeZone() {
        return this.timeZone;
    }

    @Override
    public Config getConfig() {
        return this.config;
    }
}