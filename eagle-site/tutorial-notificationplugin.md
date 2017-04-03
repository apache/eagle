---
layout: doc
title:  "Notification Plugin"
permalink: /docs/tutorial/notificationplugin.html
---

*Since Apache Eagle 0.4.0-incubating. Apache Eagle will be called Eagle in the following.*

### Eagle Notification Plugins

[Eagle Notification Plugin](https://cwiki.apache.org/confluence/display/EAG/Alert+notification+plugin) provides an interface for users to consume Eagle alerts. When define a policy, a user can add an arbitrary number of notification plugin instances. By default, Eagle supports three types of notification: EagleStore, Kafka[^KAFKA] and Email.

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

**Note**: `fields` is the configuration for notification type `kafka`

How can we do that? [Here](https://github.com/apache/eagle/blob/master/eagle-assembly/src/main/bin/eagle-topology-init.sh) are Eagle other notification plugin configurations. Just append yours to it, and run this script when Eagle service is up. 



---

#### *Footnotes*

[^KAFKA]:*All mentions of "kafka" on this page represent Apache Kafka.*
