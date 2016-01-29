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

import org.apache.eagle.datastream.ExecutionEnvironments;
import org.apache.eagle.datastream.core.StreamProducer;
import org.apache.eagle.datastream.storm.StormExecutionEnvironment;

/**
 * Created on 1/12/16.
 */
public class HadoopJmxMetricMonitor {

    public static void main(String[] args) {
        StormExecutionEnvironment env = ExecutionEnvironments.get(args, StormExecutionEnvironment.class);
        String streamName = "hadoopJmxMetricEventStream";
        StreamProducer sp = env.fromSpout(Utils.createProvider(env.getConfig())).withOutputFields(2).nameAs(streamName);
        sp.alertWithConsumer(streamName, "hadoopJmxMetricAlertExecutor");

        env.execute();
    }

}
