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
package eagle.common.email;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import eagle.common.DateTimeUtil;
import junit.framework.Assert;

import org.apache.velocity.VelocityContext;

public class TestEagleMailClient {

	@SuppressWarnings("serial")
//	@Test
	public void testCreateTemplate() {
		EagleMailClient eagleMailService = new EagleMailClient();
		VelocityContext context = new VelocityContext();
		context.put("list", new ArrayList<HashMap<String, String>>() {
			{
				add(new HashMap<String, String>() {
					{
						put("hostname", "a");
						put("date_reception", "1");
						put("type", "1");
						put("origin", "1");
						put("msg", "1");
					}
				});
				add(new HashMap<String, String>() {
					{
						put("hostname", "a");
						put("date_reception", "1");
						put("type", "1");
						put("origin", "1");
						put("msg", "1");
					}
				});
			}
		});
		
		context.put("statistics", new HashMap<String, String>() {
			{
				put("hostname", "1");
				put("date_reception", "1");
				put("type", "1");
				put("origin", "1");
				put("msg", "1");
			}});

//		Assert.assertEquals(eagleMailService.send("eagle@123.dc1.xyz.com",
//				"xinzli@xyz.com", "", "[warn]can not find hostname", "tec_alert.vm",
//				context), true);
		Assert.assertEquals(eagleMailService.send("hchen9@xyz.com",
				"hchen9@xyz.com", "", "[warn]can not find hostname", "tec_alert.vm",
				context), true);
	}
	
	
	@SuppressWarnings("serial")
//	@Test
	public void testJPAAnomalyTemplate() {
		EagleMailClient eagleMailService = new EagleMailClient();
		VelocityContext context = new VelocityContext();
		context.put("startTime", DateTimeUtil.secondsToHumanDate(System.currentTimeMillis() / 1000));
		context.put("endTime", DateTimeUtil.secondsToHumanDate(System.currentTimeMillis() / 1000));
		context.put("cluster", "cluster1");
		context.put("datacenter", "dc1");
		context.put("count", "1001");
		
		context.put("configMap", new HashMap<String, String>() {
			{
				put("para1", "value1");
				put("para2", "value2");
				put("para3", "value3");
			}});
		
		context.put("configDescMap", new HashMap<String, String>() {
			{
				put("para1", "desc1");
				put("para2", "desc2");
				put("para3", "desc3");
			}});
		
		context.put("anomalyHostMap", new HashMap<String, String>() {
			{
				put("host1", "anomalydesc1");
				put("host2", "anomalydesc2");
				put("host3", "anomalydesc3");
			}});
		
		context.put("errorMap", new HashMap<String, String>() {
			{
				put("host1", "error1");
				put("host2", "error2");
				put("host3", "error3");
			}});
		
		context.put("jobNameMap", new HashMap<String, String>() {
			{
				put("host1", "job1");
				put("host2", "job2");
				put("host3", "job3");
			}});
		
		Assert.assertEquals(eagleMailService.send("noreply-hadoop-eagle@xyz.com",
				"xinzli@xyz.com", null, "Alert email test - just test, pls ignore", "test_anomaly_alert.vm",
				context), true);
	}


	@SuppressWarnings("serial")
//	@Test
	public void testCreateTemplateWithImage() {
		EagleMailClient eagleMailService = new EagleMailClient();
		VelocityContext context = new VelocityContext();
		context.put("list", new ArrayList<HashMap<String, String>>() {
			{
				add(new HashMap<String, String>() {
					{
						put("hostname", "a");
						put("date_reception", "1");
						put("type", "1");
						put("origin", "1");
						put("msg", "1");
					}
				});
				add(new HashMap<String, String>() {
					{
						put("hostname", "a");
						put("date_reception", "1");
						put("type", "1");
						put("origin", "1");
						put("msg", "1");
					}
				});
			}
		});

		context.put("statistics", new HashMap<String, String>() {
			{
				put("hostname", "1");
				put("date_reception", "1");
				put("type", "1");
				put("origin", "1");
				put("msg", "1");
			}});

		Map<String,File> attach = new HashMap<String,File>();
		attach.put("chart.png",new File(TestEagleMailClient.class.getClassLoader().getResource("test_embed.png").getFile()));
		Assert.assertEquals(eagleMailService.send("hchen9@xyz.com",
				"hchen9@xyz.com", "", "[warn]can not find hostname", "tec_alert.vm",
				context,attach), true);
	}
	
//	@Test
//	public void testEagleMailClient() {
//		EagleMailClient client = new EagleMailClient();
//		client.send("libsun@xyz.com", "libsun@xyz.com", "", "test", "");
//	}
}
