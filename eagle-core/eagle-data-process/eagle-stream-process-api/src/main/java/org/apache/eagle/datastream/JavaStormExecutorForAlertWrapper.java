/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.datastream;

import com.typesafe.config.Config;

import java.util.List;
import java.util.SortedMap;

public class JavaStormExecutorForAlertWrapper extends JavaStormStreamExecutor3<String, String, SortedMap<Object, Object>>{
    private JavaStormStreamExecutor<Tuple2<String, SortedMap<Object, Object>>> delegate;
    private String streamName;
    public JavaStormExecutorForAlertWrapper(JavaStormStreamExecutor<Tuple2<String, SortedMap<Object, Object>>> delegate, String streamName){
        this.delegate = delegate;
        this.streamName = streamName;
    }
    @Override
    public void prepareConfig(Config config) {
        delegate.prepareConfig(config);
    }

    @Override
    public void init() {
        delegate.init();
    }

    @Override
    public void flatMap(List<Object> input, final Collector<Tuple3<String, String, SortedMap<Object, Object>>> collector) {
        Collector delegateCollector = new Collector(){
            @Override
            public void collect(Object o) {
                Tuple2 tuple2 = (Tuple2)o;
                collector.collect(new Tuple3(tuple2.f0(), streamName, tuple2.f1()));
            }
        };
        delegate.flatMap(input, delegateCollector);
    }
}
