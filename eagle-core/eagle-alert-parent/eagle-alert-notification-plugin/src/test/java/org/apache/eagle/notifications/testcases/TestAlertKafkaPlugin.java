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

package org.apache.eagle.notifications.testcases;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import junit.framework.Assert;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.metric.AlertContext;
import org.apache.eagle.notification.plugin.AlertKafkaPlugin;
import org.apache.eagle.policy.common.Constants;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestAlertKafkaPlugin {
	@Ignore // only work when kafka is ready for use
	@Test
	public void testAlertToKafkaBus() throws Exception
	{
		AlertKafkaPlugin plugin = new AlertKafkaPlugin();
		Config config = ConfigFactory.load();
		AlertDefinitionAPIEntity def = new AlertDefinitionAPIEntity();
		def.setTags(new HashMap<String, String>());
		def.getTags().put(Constants.POLICY_ID, "testPolicyId");
		def.setNotificationDef("[{\"notificationType\":\"kafka\",\"kafka_broker\":\"sandbox.hortonworks.com:6667\",\"topic\":\"sandbox_hdfs_audit_log\"}]");
		plugin.init(config, Arrays.asList(def));

		AlertAPIEntity alert = new AlertAPIEntity();
		alert.setTags(new HashMap<String, String>());
		alert.getTags().put(Constants.POLICY_ID, "testPolicyId");
		alert.setDescription("");
		alert.setAlertContext(new AlertContext().toJsonString());
		plugin.onAlert(alert);
		Thread.sleep(1000); // wait for message sent out
		Assert.assertTrue(plugin.getStatusList().get(0).successful);
	}
}
