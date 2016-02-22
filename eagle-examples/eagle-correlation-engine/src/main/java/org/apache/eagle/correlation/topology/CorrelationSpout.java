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
package org.apache.eagle.correlation.topology;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import kafka.serializer.StringDecoder;

import org.apache.eagle.correlation.client.IMetadataClient;
import org.apache.eagle.correlation.client.MetadataClientImpl;

import org.apache.eagle.correlation.topology.*;
import storm.kafka.BrokerHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

/**
 * Created by yonzhang on 2/18/16. Wrap KafkaSpout and manage metric metadata
 */
public class CorrelationSpout extends BaseRichSpout {
	// topic to KafkaSpoutWrapper
	private Map<String, KafkaSpoutWrapper> kafkaSpoutList = new HashMap<>();
	private int numBolts;

	public CorrelationSpout(int numBolts) {
		this.numBolts = numBolts;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		for(int i=0; i<numBolts; i++) {
			declarer.declareStream("stream_" + i, new Fields("topic","f1"));
		}
	}

	private KafkaSpoutWrapper createSpout(Map conf, TopologyContext context,
			SpoutOutputCollector collector, String topic) {
		BrokerHosts hosts = new ZkHosts("localhost:2181");
		// write partition offset etc. into zkRoot+id
		// see PartitionManager.committedPath
		SpoutConfig config = new SpoutConfig(hosts, topic,
				"/eaglecorrelationconsumers", "testspout_" + topic);
		config.scheme = new SchemeAsMultiScheme(new MyScheme(topic));
		KafkaSpoutWrapper wrapper = new KafkaSpoutWrapper(config);
		SpoutOutputCollectorWrapper collectorWrapper = new SpoutOutputCollectorWrapper(
				collector, numBolts);
		wrapper.open(conf, new TopologyContextWrapper(context, topic),
				collectorWrapper);
		return wrapper;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		Config config = ConfigFactory.load();
		IMetadataClient client = new MetadataClientImpl(config);
		List<String> topics = client.findAllTopics();
		//
		System.out.println(topics);
		//
		for (String topic : topics) {
			System.out.println(topic);
			CreateTopicUtils.ensureTopicReady(topic);
			KafkaSpoutWrapper wrapper = createSpout(conf, context, collector,
					topic);
			kafkaSpoutList.put(topic, wrapper);
		}
	}

	@Override
	public void nextTuple() {
		for (KafkaSpoutWrapper wrapper : kafkaSpoutList.values()) {
			wrapper.nextTuple();
		}
	}

	/**
	 * find the correct wrapper to do ack that means msgId should be mapped to
	 * wrapper
	 * 
	 * @param msgId
	 */
	@Override
	public void ack(Object msgId) {
		// decode and get topic
		KafkaMessageIdWrapper id = (KafkaMessageIdWrapper) msgId;
		System.out.println("acking message " + msgId + ", with topic "
				+ id.topic);
		KafkaSpoutWrapper spout = kafkaSpoutList.get(id.topic);
		spout.ack(id.id);
	}

	@Override
	public void fail(Object msgId) {
		// decode and get topic
		KafkaMessageIdWrapper id = (KafkaMessageIdWrapper) msgId;
		System.out.println("failing message " + msgId + ", with topic "
				+ id.topic);
		KafkaSpoutWrapper spout = kafkaSpoutList.get(id.topic);
		spout.ack(id.id);
	}

	@Override
	public void deactivate() {
		System.out.println("deactivate");
		for (KafkaSpoutWrapper wrapper : kafkaSpoutList.values()) {
			wrapper.deactivate();
		}
	}

	@Override
	public void close() {
		System.out.println("close");
		for (KafkaSpoutWrapper wrapper : kafkaSpoutList.values()) {
			wrapper.close();
		}
	}

	public static class MyScheme implements Scheme {
		private String topic;

		public MyScheme(String topic) {
			this.topic = topic;
		}

		@Override
		public List<Object> deserialize(byte[] ser) {
			StringDecoder decoder = new StringDecoder(
					new kafka.utils.VerifiableProperties());
			Object log = decoder.fromBytes(ser);
			return Arrays.asList(topic, log);
		}

		@Override
		public Fields getOutputFields() {
			return new Fields("f1");
		}
	}
}
