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

import eagle.security.util.ExternalDataJoiner;
import junit.framework.Assert;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.HashMap;
import java.util.Map;


public class TestOuterDataJoiner {
	private Map<String, Object> prop = new HashMap<String, Object>();

	public static class TestJob implements Job{
		@Override
		public void execute(JobExecutionContext context)
				throws JobExecutionException {
			JobDataMap map = context.getJobDetail().getJobDataMap();
			Assert.assertEquals("a", map.get("key1"));
			Assert.assertEquals(25, map.getInt("key2"));
			System.out.println("validated");
		}
	}
	
	//@Before
	public void setup(){
		prop.put("key1", "a");
		prop.put("key2", 25);
	}
	
	//@Test
	public void testStartStop() throws Exception{
		ExternalDataJoiner joiner = new ExternalDataJoiner(TestJob.class, prop);
		joiner.start();
		Thread.sleep(1000);
		joiner.stop();
	}
}
*/
