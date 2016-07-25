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


import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class PublishSpecData {
    private static Broadcast<PublishSpec> instance = null;

    public static Broadcast<PublishSpec> getInstance(JavaSparkContext jsc, final PublishSpec meta) {
        if (instance == null) {
            synchronized (PublishSpecData.class) {
                if (instance == null) {
                    instance = jsc.broadcast(meta);
                }
            }
        } else {
            instance = jsc.broadcast(meta);
        }
        return instance;
    }
}
