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
package org.apache.eagle.alert.persist;

import org.apache.eagle.alert.entity.AlertAPIEntity;
import com.typesafe.config.Config;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor1;
import org.apache.eagle.datastream.Tuple1;

import java.util.Arrays;

public class AlertPersistExecutor extends JavaStormStreamExecutor1<String> {

	private static final long serialVersionUID = 1L;
	private Config config;
	private EaglePersist persist;

	public AlertPersistExecutor(){
	}
    @Override
	public void prepareConfig(Config config) {
		this.config = config;		
	}

    @Override
	public void init() {
		String host = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
		int port = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);
		String username = config.hasPath(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME)
				? config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME) : null;
		String password = config.hasPath(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD)
				? config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD) : null;
		this.persist = new EaglePersist(host, port, username, password);
	}

    @Override
    public void flatMap(java.util.List<Object> input, Collector<Tuple1<String>> outputCollector){
        persist.doPersist(Arrays.asList((AlertAPIEntity)(input.get(1))));
    }
}
