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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.correlation.client.MetadataClientImpl;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * Created by yonzhang on 2/18/16.
 */
class MetadataLoader implements Runnable {

	private Map<String, List<String>> metadata;
	private MetadataClientImpl metadata_obj;

	public MetadataLoader(Config config) {
		metadata = new HashMap<String, List<String>>();
		metadata_obj = new MetadataClientImpl(config);
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		// connect to meta-data server and pull data
		try {
			while (true) {
				/*
				 * ArrayList<String> arr1 = new ArrayList<String>();
				 * arr1.add("x1"); arr1.add("y1"); arr1.add("z1");
				 * metadata.put("G1", arr1); ArrayList<String> arr2 = new
				 * ArrayList<String>(); arr2.add("x1"); arr2.add("y2");
				 * arr2.add("z2"); metadata.put("G2", arr2); Thread.sleep(60 *
				 * 1000);
				 */
				metadata = metadata_obj.findAllGroups();
				Thread.sleep(60*1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public Map<String, List<String>> getMetadata() {
		return metadata;
	}
}

class Dispatcher {
	private Map<String, List<TopicGroupEvaluator>> topic_groups; // topic to
														// list of
														// groups
	private Map<String, List<String>> metadata; // group to list of
												// topics mapping
	private HashMap<String, TopicGroupEvaluator> grp_name_topic_obj;

	Dispatcher(Map<String, List<String>> metadata) {
		topic_groups = new HashMap<String, List<TopicGroupEvaluator>>();
		this.metadata = metadata;
		grp_name_topic_obj = new HashMap<String, TopicGroupEvaluator>();
	}

	public void dispatch(Tuple input) {
		Iterator it = metadata.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			String grp_name = (String) pair.getKey();
			ArrayList<String> topics = (ArrayList<String>) pair.getValue();
			// key=grpname and value=list of topics
			TopicGroupEvaluator t_grp;
			if (!(grp_name_topic_obj.containsKey(grp_name)))
				t_grp = new TopicGroupEvaluator(grp_name, topics);
			else
				t_grp = grp_name_topic_obj.get(grp_name);

			for (int i = 0; i < topics.size(); i++) {
				String topic_name = topics.get(i);
				List<TopicGroupEvaluator> t_groups = new ArrayList<TopicGroupEvaluator>();

				if (topic_groups.containsKey(topic_name))
					t_groups = topic_groups.get(topic_name);
				t_groups.add(t_grp); // add current topic group
				topic_groups.put(topic_name, t_groups);
			}
		}
	}

	public void printTopicGroups() {

		Iterator it = topic_groups.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			String topic_name = (String) pair.getKey();
			ArrayList<TopicGroupEvaluator> grps = (ArrayList<TopicGroupEvaluator>) pair.getValue();
			for (int i = 0; i < grps.size(); i++)
				System.out.println("key=" + topic_name + " value=" + grps.get(i).getName());
		}
	}
}

public class TopicBolt implements IRichBolt {
	private OutputCollector collector;
	private MetadataLoader metadata_loader_obj;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		// create a new thread to download meta-data from service thread runs
		// every minute
		Config config = ConfigFactory.load(); // should be passed from starter
		metadata_loader_obj = new MetadataLoader(config);
		Thread t = new Thread(metadata_loader_obj);
		t.start();
	}

	@Override
	public void execute(Tuple input) {
		collector.ack(input);
		System.out.println("tuple is coming: " + input);
		// get the topic and group of the input message
		// List<Object> field_values = input.getValues();
		// String topic_name = (String) field_values.get(0);

		// call dispatcher

		Dispatcher d = new Dispatcher(metadata_loader_obj.getMetadata());
		d.dispatch(input);
		d.printTopicGroups();

	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
