package org.apache.eagle.alert.metric.reporter;

import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.eagle.alert.metric.entity.MetricEvent;
import org.apache.eagle.alert.utils.ByteUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

public class KafkaReporter extends ScheduledReporter {
	private final static Logger LOG = LoggerFactory.getLogger(KafkaReporter.class);
	private final String topic;
	private final Properties properties;
	private final Producer<byte[], String> producer;
	private final Map<String, Object> additionalFields;

	protected KafkaReporter(MetricRegistry registry, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, String topic, Properties config, Map<String, Object> additionalFields) {
		super(registry, "kafka-reporter", filter, rateUnit, durationUnit);
		this.topic = topic;
		this.properties = new Properties();
		Preconditions.checkNotNull(topic,"topic should not be null");
//		properties.put("bootstrap.servers", brokerList);
//		properties.put("metadata.broker.list", brokerList);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("request.required.acks", "1");
		properties.put("key.deserializer","org.apache.kafka.common.serialization.ByteArraySerializer");
		properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		if(config != null) {
			LOG.info(config.toString());
			properties.putAll(config);
		}
		this.additionalFields = additionalFields;
		this.producer = new KafkaProducer<>(properties);
		LOG.info("Initialized kafka-reporter");
	}

	@SuppressWarnings("rawtypes")
    @Override
	public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
		for(SortedMap.Entry<String, Gauge> entry:gauges.entrySet()){
			onMetricEvent(MetricEvent.of(entry.getKey()).from(entry.getValue()).build());
		}
		for(SortedMap.Entry<String, Counter> entry:counters.entrySet()){
			onMetricEvent(MetricEvent.of(entry.getKey()).from(entry.getValue()).build());
		}
		for(SortedMap.Entry<String, Histogram> entry:histograms.entrySet()){
			onMetricEvent(MetricEvent.of(entry.getKey()).from(entry.getValue()).build());
		}
		for(SortedMap.Entry<String, Meter> entry:meters.entrySet()){
			onMetricEvent(MetricEvent.of(entry.getKey()).from(entry.getValue()).build());
		}
		for(SortedMap.Entry<String, Timer> entry:timers.entrySet()){
			onMetricEvent(MetricEvent.of(entry.getKey()).from(entry.getValue()).build());
		}
	}

	private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private void onMetricEvent(MetricEvent event){
		try {
			if(additionalFields!=null){
				event.putAll(additionalFields);
			}
			// TODO: Support configurable partition key
			byte[] key = ByteUtils.intToBytes(event.hashCode());
			ProducerRecord<byte[],String> record  = new ProducerRecord<>(topic, key, OBJECT_MAPPER.writeValueAsString(event));
			// TODO: Support configuration timeout
			this.producer.send(record).get(5,TimeUnit.SECONDS);
		} catch (JsonProcessingException e) {
			LOG.error("Failed to serialize {} as json",event,e);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			LOG.error("Failed to produce message to topic {}",topic,e);
		}
	}

	@Override
	public void stop() {
		this.producer.close();
		super.stop();
	}

	@Override
	public void close() {
		this.producer.close();
		super.close();
	}

	public static Builder forRegistry(MetricRegistry registry){
		return new Builder(registry);
	}

	public static class Builder{
		private final MetricRegistry registry;
		private TimeUnit rateUnit;
		private TimeUnit durationUnit;
		private MetricFilter filter;
		private String topic;
		private Properties properties;
		private Map<String, Object> additionalFields;

		private Builder(MetricRegistry registry) {
			this.registry = registry;
			this.rateUnit = TimeUnit.SECONDS;
			this.durationUnit = TimeUnit.MILLISECONDS;
			this.filter = MetricFilter.ALL;
		}

		/**
		 * Convert rates to the given time unit.
		 *
		 * @param rateUnit a unit of time
		 * @return {@code this}
		 */
		public Builder convertRatesTo(TimeUnit rateUnit) {
			this.rateUnit = rateUnit;
			return this;
		}

		/**
		 * Convert durations to the given time unit.
		 *
		 * @param durationUnit a unit of time
		 * @return {@code this}
		 */
		public Builder convertDurationsTo(TimeUnit durationUnit) {
			this.durationUnit = durationUnit;
			return this;
		}

		/**
		 * Only report metrics which match the given filter.
		 *
		 * @param filter a {@link MetricFilter}
		 * @return {@code this}
		 */
		public Builder filter(MetricFilter filter) {
			this.filter = filter;
			return this;
		}

		public Builder topic(String topic){
			this.topic = topic;
			return this;
		}

		public Builder config(Properties properties){
			this.properties = properties;
			return this;
		}

		/**
		 * Builds a {@link ConsoleReporter} with the given properties.
		 *
		 * @return a {@link ConsoleReporter}
		 */
		public KafkaReporter build() {
			if(topic == null && properties!=null) topic = properties.getProperty("topic");
			return new KafkaReporter(registry,filter,rateUnit,durationUnit,topic,properties,additionalFields);
		}

		@SuppressWarnings("serial")
        public Builder config(Config config) {
			this.config(new Properties(){{
				putAll(config.root().unwrapped());
			}});
			return this;
		}

		public Builder addFields(Map<String, Object> tags) {
			this.additionalFields = tags;
			return this;
		}
	}
}