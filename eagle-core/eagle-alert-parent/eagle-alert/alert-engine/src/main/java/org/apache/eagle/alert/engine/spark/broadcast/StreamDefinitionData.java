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
package org.apache.eagle.alert.engine.spark.broadcast;

import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Map;

public class StreamDefinitionData {
    private static Broadcast<Map<String, StreamDefinition>> instance = null;

    public static Broadcast<Map<String, StreamDefinition>> getInstance(JavaSparkContext jsc, Map<String, StreamDefinition> sds ) {
        if (instance == null) {
            synchronized (StreamDefinitionData.class) {
                if (instance == null) {
                    instance = jsc.broadcast(sds);
                }
            }
        }else{
            instance = jsc.broadcast(sds);
        }
        return instance;
    }
}