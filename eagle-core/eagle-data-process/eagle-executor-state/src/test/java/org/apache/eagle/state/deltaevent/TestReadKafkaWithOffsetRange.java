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

package org.apache.eagle.state.deltaevent;

import java.util.*;

/**
 * Read kafka message for a specific offset range
 */
public class TestReadKafkaWithOffsetRange {
    public static void main(String[] args) throws Exception{
        int port = 6667;
        String topic = "executorStateTopic_sandbox_eventSource";
        int partition = 1;
        KafkaReadWithOffsetRange test = new KafkaReadWithOffsetRange(Collections.singletonList("localhost"), port, topic, partition, new DeltaEventValueDeserializer());
        test.readUntilMaxOffset(300, new DeltaEventReplayCallback() {
            @Override
            public void replay(Object event) {
                System.out.println(event);
            }
        });
    }
}
