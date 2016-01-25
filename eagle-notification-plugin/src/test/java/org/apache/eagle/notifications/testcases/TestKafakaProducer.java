package org.apache.eagle.notifications.testcases;

import org.apache.eagle.notification.KafkaProducerSingleton;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Test;

public class TestKafakaProducer {

	@Test
	public void test() throws Exception{
		KafkaProducer<?, ?> producer = KafkaProducerSingleton.INSTANCE.getProducer();
		System.out.println(producer.toString());
		System.out.println(producer.metrics());
	}
}
