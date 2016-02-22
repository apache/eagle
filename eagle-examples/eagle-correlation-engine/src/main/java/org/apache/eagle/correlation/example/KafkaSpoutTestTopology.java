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
package org.apache.eagle.correlation.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import kafka.serializer.StringDecoder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.Arrays;
import java.util.List;

/**
 * Created on 2/17/16.
 * this example demostrate how KafkaSpout works
 */
public class KafkaSpoutTestTopology {
    public static void main(String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();
        BrokerHosts hosts = new ZkHosts("localhost:2181");
        SpoutConfig config = new SpoutConfig(hosts, "correlationtopic", "/eaglecorrelationconsumers", "testspout");
        config.scheme = new SchemeAsMultiScheme(new MyScheme());
        KafkaSpout spout = new KafkaSpout(config);
        builder.setSpout("testSpout", spout);
        BoltDeclarer declarer  = builder.setBolt("testBolt", new TestBolt());
        declarer.fieldsGrouping("testSpout", new Fields("f1"));
        StormTopology topology = builder.createTopology();
        boolean localMode = true;
        Config conf = new Config();
        if(!localMode){
            StormSubmitter.submitTopologyWithProgressBar("testTopology", conf, topology);
        }else{
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("testTopology", conf, topology);
            while(true) {
                try {
                    Utils.sleep(Integer.MAX_VALUE);
                } catch(Exception ex) {
                }
            }
        }
    }

    public static class MyScheme implements Scheme {
        @Override
        public List<Object> deserialize(byte[] ser) {
            StringDecoder decoder = new StringDecoder(new kafka.utils.VerifiableProperties());
            Object log = decoder.fromBytes(ser);
            return Arrays.asList(log);
        }

        @Override
        public Fields getOutputFields() {
            return new Fields("f1");
        }
    }


}
