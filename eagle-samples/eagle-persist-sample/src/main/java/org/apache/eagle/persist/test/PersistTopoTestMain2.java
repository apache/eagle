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
package org.apache.eagle.persist.test;

import org.apache.eagle.dataproc.impl.storm.StormSpoutProvider;
import org.apache.eagle.datastream.ExecutionEnvironments;
import org.apache.eagle.datastream.core.StorageType;
import org.apache.eagle.datastream.core.StreamProducer;
import org.apache.eagle.datastream.storm.StormExecutionEnvironment;
import org.apache.eagle.partition.PartitionStrategy;

/**
 * Created on 1/10/16.
 */
public class PersistTopoTestMain2 {

    public static void main(String[] args) {
        System.setProperty("config.resource", "application.conf");// customize the application.conf
        StormExecutionEnvironment env = ExecutionEnvironments.getStorm();
        StormSpoutProvider provider = PersistTopoTestMain.createProvider(env.getConfig());
        exec(env, provider);
    }

    private static void exec(StormExecutionEnvironment env, StormSpoutProvider provider) {
        StreamProducer source = env.fromSpout(provider).withOutputFields(4).nameAs("kafkaMsgConsumer");
        StreamProducer filter = source;

        // "timestamp", "host", "cpu", "mem"
        String cql = " define stream eagleQuery(eagleAlertContext object, timestamp long, host string, cpu int, mem long);"
                + " @info(name='query')"
                + " from eagleQuery#window.externalTime(timestamp, 10 min) "
                + " select eagleAlertContext, min(timestamp) as starttime, avg(cpu) as avgCpu, avg(mem) as avgMem insert into tmp;";
        StreamProducer aggregate = filter.aggregate(cql, new PartitionStrategy() {
            @Override
            public int balance(String key, int buckNum) {
                return 0;
            }
        });

        StreamProducer persist = aggregate.persist("persistExecutor1", StorageType.KAFKA());

        env.execute();
    }
}
