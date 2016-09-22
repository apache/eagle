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

package org.apache.eagle.topology.extractor.hbase;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;
import org.apache.eagle.topology.TopologyCheckAppConfig;
import org.apache.eagle.topology.TopologyCheckMessageId;
import org.apache.eagle.topology.TopologyConstants;
import org.apache.eagle.topology.extractor.TopologyEntityParserResult;
import org.apache.eagle.topology.extractor.TopologyCrawler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HbaseTopologyCrawler implements TopologyCrawler {

    private static final Logger LOG = LoggerFactory.getLogger(HbaseTopologyCrawler.class);

    private HbaseTopologyEntityParser parser;
    private SpoutOutputCollector collector;

    public HbaseTopologyCrawler(TopologyCheckAppConfig config, SpoutOutputCollector collector) {
        this.parser = new HbaseTopologyEntityParser(config.dataExtractorConfig.site, config.hBaseConfig);
        this.collector = collector;
    }

    @Override
    public void extract() {
        long updateTimestamp = System.currentTimeMillis();
        TopologyEntityParserResult result = null;
        try {
            result = parser.parse(updateTimestamp);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        if (result == null || result.getMasterNodes().isEmpty()) {
            LOG.warn("No data fetched");
            return;
        }
        TopologyCheckMessageId messageId = new TopologyCheckMessageId(TopologyConstants.TopologyType.HBASE, updateTimestamp);
        this.collector.emit(new Values(TopologyConstants.HBASE_INSTANCE_SERVICE_NAME, result), messageId);
    }
}
