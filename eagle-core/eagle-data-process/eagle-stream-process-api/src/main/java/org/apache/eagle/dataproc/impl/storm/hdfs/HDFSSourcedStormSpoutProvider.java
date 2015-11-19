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
package org.apache.eagle.dataproc.impl.storm.hdfs;

import com.typesafe.config.Config;
import org.apache.eagle.dataproc.impl.storm.AbstractStormSpoutProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.base.BaseRichSpout;

public class HDFSSourcedStormSpoutProvider extends AbstractStormSpoutProvider {
	private static final Logger LOG = LoggerFactory.getLogger(HDFSSourcedStormSpoutProvider.class);
	
	public abstract static class HDFSSpout extends BaseRichSpout{
		public abstract void copyFiles(); 
		public void fail(Object msgId) {
		    int transactionId = (Integer) msgId;
		    LOG.info(transactionId + " failed");
		}
		
		public void ack(Object msgId) {
		    int transactionId = (Integer) msgId;
		    LOG.info(transactionId + " acknowledged");
		}
		
		public static HDFSSpout getHDFSSpout(String conf, Config configContext){
			if(conf.equalsIgnoreCase("data collection")){
				return new DataCollectionHDFSSpout(configContext); 
			}
			if(conf.equalsIgnoreCase("user profile generation")){
				return new UserProfileGenerationHDFSSpout(configContext); 
			}
			return null;
		}
	}
	
	@Override
	public BaseRichSpout getSpout(Config context){
		LOG.info("GetHDFSSpout called");
		String typeOperation = context.getString("dataSourceConfig.typeOperation");
		HDFSSpout spout = HDFSSpout.getHDFSSpout(typeOperation, context);
		spout.copyFiles();
		return spout;
	}
}