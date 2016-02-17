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
package org.apache.eagle.example.persist;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.typesafe.config.Config;
import org.apache.eagle.dataproc.impl.storm.StormSpoutProvider;
import org.apache.eagle.datastream.ExecutionEnvironments;
import org.apache.eagle.datastream.core.StorageType;
import org.apache.eagle.datastream.core.StreamProducer;
import org.apache.eagle.datastream.storm.StormExecutionEnvironment;
import org.apache.eagle.partition.PartitionStrategy;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;

/**
 * Created on 1/4/16.
 *
 * This test demonstrates how user could use the new aggregate and persist feature for case like metrics processing&storage.
 *
 */
public class PersistTopoTestMain {

    public static void main(String[] args) {
//        System.setProperty("config.resource", "application.conf");
        StormExecutionEnvironment env = ExecutionEnvironments.getStorm();
        StormSpoutProvider provider = createProvider(env.getConfig());
        execWithDefaultPartition(env, provider);
    }

    @SuppressWarnings("unchecked")
    public static void execWithDefaultPartition(StormExecutionEnvironment env, StormSpoutProvider provider) {
        StreamProducer source = env.fromSpout(provider).withOutputFields(2).nameAs("kafkaMsgConsumer");
        StreamProducer filter = source;

        // required : persistTestEventStream schema be created in metadata manager
        // required : policy for aggregateExecutor1 be created in metadata manager
        StreamProducer aggregate = filter.aggregate(Arrays.asList("persistTestEventStream"), "aggregateExecutor1", new PartitionStrategy() {
            @Override
            public int balance(String key, int buckNum) {
                return 0;
            }
        });

        StreamProducer persist = aggregate.persist("persistExecutor1", StorageType.KAFKA());

        env.execute();
    }

    public static StormSpoutProvider createProvider(Config config) {

        return new StormSpoutProvider(){

            @Override
            public BaseRichSpout getSpout(Config context) {
                return new StaticMetricSpout();
            }
        };
    }

    public static class StaticMetricSpout extends BaseRichSpout {

        private long base;
        private SpoutOutputCollector collector;

        public StaticMetricSpout() {
            base = System.currentTimeMillis();
        }

        private Random cpuRandom = new Random();
        private Random memRandom = new Random();
        private static final long FULL_MEM_SIZE_BYTES = 512  * 1024 * 1024 * 1024;// 16g memory upbound limit

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("timestamp", "host", "cpu", "mem"));
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(100);
            base = base + 100;// with fix steps..
            long mem = Double.valueOf(memRandom.nextGaussian() * FULL_MEM_SIZE_BYTES).longValue();
            collector.emit(new Values(base, "host", cpuRandom.nextInt(100), mem));
        }
    }

}
