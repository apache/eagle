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

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.alert.engine.spark.model.KafkaClusterInfo;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;


public class GetOffsetRangeFunction implements VoidFunction2<OffsetRange[], KafkaClusterInfo> {


    private static final Logger LOG = LoggerFactory.getLogger(GetOffsetRangeFunction.class);
    private AtomicReference<Map<KafkaClusterInfo, OffsetRange[]>> offsetRangesClusterMapRef;

    public GetOffsetRangeFunction(AtomicReference<Map<KafkaClusterInfo, OffsetRange[]>> offsetRangesClusterMapRef) {
        this.offsetRangesClusterMapRef = offsetRangesClusterMapRef;
    }

    @Override
    public void call(OffsetRange[] offsetRanges, KafkaClusterInfo kafkaClusterInfo) throws Exception {
        Map<KafkaClusterInfo, OffsetRange[]> offsetRangeMap = offsetRangesClusterMapRef.get();
        if (offsetRangeMap == null) {
            offsetRangeMap = Maps.newHashMap();
        }
        offsetRangeMap.put(kafkaClusterInfo, offsetRanges);
        offsetRangesClusterMapRef.set(offsetRangeMap);
    }
}