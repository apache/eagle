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
package org.apache.eagle.service.selfcheck;

/**
 * expose internal configuration or metrics
 */
//@XmlRootElement
//@XmlAccessorType(XmlAccessType.FIELD)
//@XmlType(propOrder = {"env", "hbaseZookeeperQuorum", "hbaseZookeeperClientPort"})
public class EagleServiceSelfCheckAPIEntity {
	private String env;
	private String hbaseZookeeperQuorum;
	private String hbaseZookeeperClientPort;
	public String getEnv() {
		return env;
	}
	public void setEnv(String env) {
		this.env = env;
	}
	public String getHbaseZookeeperQuorum() {
		return hbaseZookeeperQuorum;
	}
	public void setHbaseZookeeperQuorum(String hbaseZookeeperQuorum) {
		this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
	}
	public String getHbaseZookeeperClientPort() {
		return hbaseZookeeperClientPort;
	}
	public void setHbaseZookeeperClientPort(String hbaseZookeeperClientPort) {
		this.hbaseZookeeperClientPort = hbaseZookeeperClientPort;
	}
}
