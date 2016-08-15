/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.alert.engine.spark.function;

import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.sorter.StreamTimeClock;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import scala.Tuple2;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class StreamWindowStoreFunction implements Function3<Integer, Optional<Map<String, StreamTimeClock>>, State<Map<String, StreamTimeClock>>, Tuple2<Integer, PartitionedEvent>> {

    private AtomicReference<Map<String, StreamTimeClock>> streamTimeClock;

    public StreamWindowStoreFunction(AtomicReference<Map<String, StreamTimeClock>> streamTimeClock) {
        this.streamTimeClock = streamTimeClock;
    }

    @Override
    public Tuple2<Integer, PartitionedEvent> call(Integer v1, Optional<Map<String, StreamTimeClock>> v2, State<Map<String, StreamTimeClock>> v3) throws Exception {
        return null;
    }
}
