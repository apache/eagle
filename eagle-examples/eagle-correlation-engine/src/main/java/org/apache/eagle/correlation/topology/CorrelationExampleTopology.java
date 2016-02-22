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

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created on 2/17/16. this example demostrate how Correlation topology works
 */
public class CorrelationExampleTopology {
	public static void main(String[] args) throws Exception {
		Config config = ConfigFactory.load();
        int numBolts = 1;//config.getInt("eagle.correlation.numBolts");
		TopologyBuilder builder = new TopologyBuilder();
		CorrelationSpout spout = new CorrelationSpout(numBolts);
		builder.setSpout("testSpout", spout);
		// BoltDeclarer declarer = builder.setBolt("testBolt", new TestBolt());

		for (int i = 0; i < numBolts; i++) {
			BoltDeclarer declarer = builder.setBolt("testBolt_" + i,
					new TopicBolt());
			declarer.fieldsGrouping("testSpout", "stream_" + i,
					new Fields("f1"));
		}
		StormTopology topology = builder.createTopology();
		boolean localMode = true;
		backtype.storm.Config conf = new backtype.storm.Config();
		if (!localMode) {
			StormSubmitter.submitTopologyWithProgressBar("testTopology", conf,
					topology);
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("testTopology", conf, topology);
			while (true) {
				try {
					Utils.sleep(Integer.MAX_VALUE);
				} catch (Exception ex) {
				}
			}
		}
	}
}
