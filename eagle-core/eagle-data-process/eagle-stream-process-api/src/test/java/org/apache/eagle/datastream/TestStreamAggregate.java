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
package org.apache.eagle.datastream;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.eagle.policy.entity.AlertAPIEntity;
import org.apache.eagle.datastream.core.FlatMapProducer;
import org.apache.eagle.datastream.core.StormSourceProducer;
import org.apache.eagle.datastream.core.StreamConnector;
import org.apache.eagle.datastream.core.StreamDAG;
import org.apache.eagle.datastream.core.StreamProducer;
import org.apache.eagle.datastream.storm.StormExecutionEnvironment;
import org.apache.eagle.partition.PartitionStrategy;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.junit.Before;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import junit.framework.Assert;
import scala.collection.Seq;


/**
 * @since Dec 18, 2015
 *
 */
public class TestStreamAggregate {

	private Config config;

	@SuppressWarnings("serial")
	private final class SimpleSpout extends BaseRichSpout {
		@SuppressWarnings("rawtypes")
		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		}
		@Override
		public void nextTuple() {
		}
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
		}
	}

	public static class TestEnvironment extends StormExecutionEnvironment {
		private static final long serialVersionUID = 1L;
		public TestEnvironment(Config conf) {
			super(conf);
		}
		@Override
		public void execute(StreamDAG dag) {
			System.out.println("DAT completed!");
		}
	}
	
	public static class DummyStrategy implements PartitionStrategy {
		private static final long serialVersionUID = 1L;
		@Override
		public int balance(String key, int buckNum) {
			return 0;
		}
	};
	
	public static class DummyExecutor extends JavaStormStreamExecutor2<String, AlertAPIEntity>  {
		@Override
		public void prepareConfig(Config config) {
		}
		@Override
		public void init() {
		}
		@Override
		public void flatMap(List input, Collector collector) {
		}
	}
	
	@Before
	public void setUp() {
		System.setProperty("config.resource", "/application.conf");
		ConfigFactory.invalidateCaches();
		config = ConfigFactory.load();
	}

	@SuppressWarnings({ "unchecked", "rawtypes", "serial" })
	@Test
	public void testAggregate1() {
		StormExecutionEnvironment exe = new TestEnvironment(config);
		
		BaseRichSpout spout = new SimpleSpout();
		StormSourceProducer ssp = exe.fromSpout(spout);
		
		ssp.flatMap(new FlatMapper<String>() {
			@Override
			public void flatMap(Seq<Object> input, Collector<String> collector) {
				// do nothing
			}
		}).aggregate(Arrays.asList("c3EsLogEventStream"), "qid", new DummyStrategy());
		
		try {
			exe.execute();
			Assert.fail("customzied flat mapper(non java storm executor) before analyze is not supported!");
		} catch (Exception e ){
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes", "serial" })
	@Test
	public void testAggregate() {
		StormExecutionEnvironment exe = new TestEnvironment(config);
		StormSourceProducer ssp = exe.fromSpout(new SimpleSpout());
		DummyExecutor dummy = new DummyExecutor();
		ssp.flatMap(dummy).aggregate(Arrays.asList("c3EsLogEventStream"), "analyzeStreamExecutor", new DummyStrategy());

		try {
			exe.execute();
		} catch (Exception e) {
			Assert.fail("customzied flat mapper before");
		}
		// Assertion
		DirectedAcyclicGraph<StreamProducer<Object>, StreamConnector<Object, Object>> dag = exe.dag();
		Assert.assertEquals("three vertex", 3, dag.vertexSet().size());
		boolean hasWrapped = false;
		for (StreamProducer<Object> obj : dag.vertexSet()) {
			if (obj instanceof FlatMapProducer) {
				if (((FlatMapProducer) obj).mapper() instanceof JavaStormExecutorForAlertWrapper) {
					hasWrapped = true;
					Assert.assertEquals("dummy executor should be wrapped in the alert wrapper func", dummy,
							((JavaStormExecutorForAlertWrapper) ((FlatMapProducer) obj).mapper() ).getDelegate());

				}
			}
		}
		Assert.assertTrue(hasWrapped);

	}
}
