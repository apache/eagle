/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.topology.extractor;

import org.apache.eagle.topology.TopologyCheckAppConfig;
import org.apache.eagle.topology.TopologyConstants;
import org.apache.eagle.topology.extractor.hbase.HbaseTopologyExtractor;

import org.slf4j.Logger;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TopologyExtractorFactory {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(TopologyExtractorFactory.class);

    private TopologyExtractorFactory() {}

    private static Map<String, Constructor<? extends TopologyExtractorBase>> extractorMap = Collections.synchronizedMap(new HashMap<>());

    private static void registerTopologyExtractor(String topologyType, Class<? extends TopologyExtractorBase> clazz) {
        Constructor<? extends TopologyExtractorBase> constructor = null;
        try {
            constructor = clazz.getConstructor(TopologyCheckAppConfig.class);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        if (constructor != null) {
            extractorMap.put(topologyType, constructor);
        }
    }

    public static TopologyExtractorBase create(TopologyConstants.TopologyType topologyType, TopologyCheckAppConfig config) {
        if (extractorMap.containsKey(topologyType.toString().toUpperCase())) {
            Constructor<? extends TopologyExtractorBase> constructor = extractorMap.get(topologyType.name());
            try {
                return constructor.newInstance(config);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            LOG.error("Unsupported topology type {}", topologyType.toString());
        }
        return null;
    }

    static {
        registerTopologyExtractor(TopologyConstants.TopologyType.HBASE.name(), HbaseTopologyExtractor.class);
        //registerTopologyExtractor(TopologyConstants.TopologyType.HDFS.name(), HdfsTopologyEntityParser.class);
        //registerTopologyExtractor(TopologyConstants.TopologyType.MR.name(), MRTopologyEntityParser.class);
    }
}
