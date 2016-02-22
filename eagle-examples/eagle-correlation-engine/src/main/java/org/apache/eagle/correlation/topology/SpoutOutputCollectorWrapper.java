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

import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.correlation.client.IMetadataClient;
import org.apache.eagle.correlation.client.MetadataClientImpl;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;

/**
 * Created by yonzhang on 2/18/16. wrap SpoutOutputCollector so Eagle can
 * intercept the message sent from within KafkaSpout collector.emit(tup, new
 * KafkaMessageId(_partition, toEmit.offset));
 */
public class SpoutOutputCollectorWrapper extends SpoutOutputCollector {
	private ISpoutOutputCollector delegate;
	private int numBolts;

	public SpoutOutputCollectorWrapper(ISpoutOutputCollector delegate,
			int numBolts) {
		super(delegate);
		this.delegate = delegate;
		this.numBolts = numBolts;
	}

	@Override
	public List<Integer> emit(List<Object> tuple, Object messageId) {
		// decode tuple to have field topic and field f1
		String topic = (String) tuple.get(0);
		System.out.println("emitted tuple: " + tuple + ", with message Id: "
				+ messageId + ", with topic " + topic);
		KafkaMessageIdWrapper newMessageId = new KafkaMessageIdWrapper(
				messageId);
		newMessageId.topic = topic;
		/*
		 * if(topic.equals("topic1")){ delegate.emit("stream1", tuple,
		 * newMessageId); delegate.emit("stream3", tuple, newMessageId); }
		 */
		Config config = ConfigFactory.load();
		IMetadataClient client = new MetadataClientImpl(config);
		Map<String, List<String>> groups = client.findAllGroups();
		for (Map.Entry<String, List<String>> e : groups.entrySet()) {
			if (e.getValue().contains(topic)) {
				delegate.emit("stream_" + e.getKey().hashCode() % numBolts,
						tuple, newMessageId);
			}
		}
		return null;
	}
}
