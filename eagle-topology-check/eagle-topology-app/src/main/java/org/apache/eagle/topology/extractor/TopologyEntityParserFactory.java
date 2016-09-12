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

import com.typesafe.config.Config;
import org.apache.eagle.topology.TopologyCheckAppConfig;
import org.apache.eagle.topology.TopologyConstants.TopologyType;
import org.apache.eagle.topology.extractor.hbase.HbaseTopologyEntityParser;
import org.apache.eagle.topology.extractor.hdfs.HdfsTopologyEntityParser;
import org.apache.eagle.topology.extractor.mr.MRTopologyEntityParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TopologyEntityParserFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TopologyEntityParserFactory.class);

    private static TopologyEntityParserFactory factory = new TopologyEntityParserFactory();
    private HashMap<String, TopologyEntityParser> parserMap;
    private final static Map<String, TopologyEntityParser> parserCache = Collections.synchronizedMap(new HashMap<>());
    private TopologyEntityParserFactory() {}

    public static TopologyEntityParserFactory getInstance(TopologyCheckAppConfig config) {
        factory.init(config);
        return factory;
    }

    public TopologyEntityParser getParserByType(TopologyType topologyType) {
        if (parserCache.containsKey(topologyType.toString())) {
            return parserCache.get(topologyType);
        } else {
            LOG.error("Illegal topology Type" + topologyType);
            return null;
        }
    }

    private void init(TopologyCheckAppConfig config) {
        parserMap = new HashMap<>();
        for (String configuredTopologyType : config.topologyTypes) {
            if (parserClazzMap.containsKey(configuredTopologyType)) {
                try {
                    TopologyEntityParser parser = parserClazzMap.get(configuredTopologyType).getConstructor(String.class, Config.class).newInstance(config.dataExtractorConfig.site, config.config);
                    parserCache.put(configuredTopologyType, parser);
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                }
            } else {
                LOG.error("Topology Topology % is not supported", configuredTopologyType);
            }
        }
    }


    private static Map<String, Class<? extends TopologyEntityParser>> parserClazzMap = Collections.synchronizedMap(new HashMap<>());

    private static void registerParser(String topologyType, Class<? extends TopologyEntityParser> clazz) {
        parserClazzMap.put(topologyType, clazz);
    }

    static {
        registerParser(TopologyType.HBASE.name(), HbaseTopologyEntityParser.class);
        registerParser(TopologyType.HDFS.name(), HdfsTopologyEntityParser.class);
        registerParser(TopologyType.MR.name(), MRTopologyEntityParser.class);
    }
}
