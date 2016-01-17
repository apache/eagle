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
package org.apache.eagle.hadoop.metric;

import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutProvider;
import org.apache.eagle.datastream.ExecutionEnvironments;
import org.apache.eagle.datastream.core.StreamProducer;
import org.apache.eagle.datastream.storm.StormExecutionEnvironment;
import org.apache.eagle.partition.PartitionStrategy;

/**
 * Created on 1/12/16.
 */
public class NameNodeLagMonitor {

    public static void main(String[] args) {
        StormExecutionEnvironment env = ExecutionEnvironments.get(args, StormExecutionEnvironment.class);
        String streamName = "s";
        StreamProducer sp = env.fromSpout(new KafkaSourcedSpoutProvider()).parallelism(1).nameAs(streamName);
        sp.aggregate("define stream s (host string, timestamp long, metric string, component string, site string, value string);" +
                        " from s#window.time(5 min) as s1 join s#window.time(5 min) as s2 on s1." +
                "",
                new PartitionStrategy() {
                    @Override
                    public int balance(String key, int buckNum) {
                        return 0;
                    }
                });

        env.execute();
    }
}
