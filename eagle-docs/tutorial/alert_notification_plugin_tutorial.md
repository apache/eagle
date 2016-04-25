<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

### Eagle Notification Plugins

[Eagle Notification Plugin](https://cwiki.apache.org/confluence/display/EAG/Alert+notification+plugin) provides an interface for users to consume Eagle alerts. When define a policy, a user can add any number of notification plugin instances. By default, Eagle supports three types of notification: EagleStore, Kafka and Email

* EagleStore: Alerts will be persisted into the underlying database via eagle. 
	* no configuration is needed. 
* Kafka: Alerts will flow into Kafka. Configurations are required:
	* **kafka_broker**: <hostname:port,..., REQUIRED: The list of hostname and hostname:port> port of the server to connect to. 
	* **topic**: kafka topic 
* email: Alert email will be sent out. Configurations are required:
	* **sender**: email sender address
	* **recipients**: email recipients, multiple email address with comma separated
	* **subject**: email subject
	
![notificationPlugin](/images/notificationPlugin.png)
### Customized Notification Plugin

To integrate a customized notification plugin, we must implement an interface 

	public interface NotificationPlugin {
    /**
     * for initialization
     * @throws Exception
     */
    void init(Config config, List<AlertDefinitionAPIEntity> initAlertDefs) throws  Exception;

    /**
     * Update Plugin if any change in Policy Definition
     * @param policy to be impacted
     * @param  notificationConfCollection
     * @throws Exception
     */
    void update(String policy, List<Map<String,String>> notificationConfCollection , boolean isPolicyDelete) throws  Exception;

    /**
     * Post a notification for the given alertEntity
     * @param alertEntity
     * @throws Exception
     */

    void onAlert(AlertAPIEntity alertEntity) throws  Exception;

    /**
     * Returns Status of Notification Post
     * @return
     */
    List<NotificationStatus> getStatusList();
	}
Examples: AlertKafkaPlugin, AlertEmailPlugin, and AlertEagleStorePlugin.

The second and crucial step is to register the configurations of the customized plugin. In other words, we need persist the configuration template into database in order to expose the configurations to users in the front end. 

Examples:

 	{
       "prefix": "alertNotifications",
       "tags": {
         "notificationType": "kafka"
       },
       "className": "org.apache.eagle.notification.plugin.AlertKafkaPlugin",
       "description": "send alert to kafka bus",
       "enabled":true,
       "fields": "[{\"name\":\"kafka_broker\",\"value\":\"sandbox.hortonworks.com:6667\"},{\"name\":\"topic\"}]"
     }
'fields' is the configuration for notification type 'kafka'

How can we do that? [Here](https://github.com/apache/incubator-eagle/blob/master/eagle-assembly/src/main/bin/eagle-topology-init.sh) are Eagle other notification plugin configurations. Just append yours to it, and run this script when Eagle service is up. 



