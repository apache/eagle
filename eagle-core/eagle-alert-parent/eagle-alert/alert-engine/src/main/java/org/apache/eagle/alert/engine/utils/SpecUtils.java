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

package org.apache.eagle.alert.engine.utils;

import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.service.SpecMetadataServiceClientImpl;

import com.typesafe.config.Config;

import java.util.Collections;
import java.util.Set;

public class SpecUtils {
    public static Set<String> getTopicsByClient(SpecMetadataServiceClientImpl client) {
        if (client == null) {
            return Collections.emptySet();
        }
        return client.getSpoutSpec().getKafka2TupleMetadataMap().keySet();
    }

    public static Set<String> getTopicsByConfig(Config config) {
        SpecMetadataServiceClientImpl client = new SpecMetadataServiceClientImpl(config);
        return SpecUtils.getTopicsByClient(client);
    }

    public static Set<String> getTopicsBySpoutSpec(SpoutSpec spoutSpec) {
        if (spoutSpec == null) {
            return Collections.emptySet();
        }
        return spoutSpec.getKafka2TupleMetadataMap().keySet();
    }
}
