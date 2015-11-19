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
 *//*

package eagle.security.dataproc;

import eagle.common.config.EagleConfigConstants;
import eagle.security.auditlog.timer.IPZonePollingJob;
import eagle.security.util.ExternalDataJoiner;

import java.util.HashMap;
import java.util.Map;


public class TestIPZonePollingJob {
	private Map<String, Object> prop = new HashMap<String, Object>();

	//@Before
	public void setup(){
		prop.put(EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST, "localhost");
		prop.put(EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT, 38080);
	}
	
	//@Test
	public void test() throws Exception{
		ExternalDataJoiner joiner = new ExternalDataJoiner(IPZonePollingJob.class, prop);
		joiner.start();
		Thread.sleep(100000);
		joiner.stop();
	}
}
*/
