package org.apache.eagle.notifications.testcases;

import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.notification.KafkaProducerSingleton;
import org.apache.eagle.notification.utils.NotificationPluginUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class PostMsg2Kafka {
	
	@Test
	public void postMessage2KafkaBus() throws Exception 
	{
		KafkaProducer<String, Object > producer = KafkaProducerSingleton.INSTANCE.getProducer();
		ProducerRecord<String, Object> record  = new ProducerRecord(NotificationPluginUtils.getPropValue("notification_topic_kafka"), "This is Log Message ");
		producer.send(record);
	}	
	
	@Test
	public void postAlertEntity2KafkaBus() throws Exception 
	{
		KafkaProducer<String, Object > producer = KafkaProducerSingleton.INSTANCE.getProducer();
		AlertAPIEntity entity = new AlertAPIEntity();
		entity.setDescription(" Sensitivity Path /hive/warehouse/data/customer_details ");
		entity.setRemediationID("100");
		ProducerRecord<String, Object> record  = new ProducerRecord(NotificationPluginUtils.getPropValue("notification_topic_kafka"), entity  );
		producer.send(record);
		System.out.println();
	}
}
