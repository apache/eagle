/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.topology.extractor.hdfs;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;
import org.apache.eagle.app.utils.AppConstants;
import org.apache.eagle.app.utils.ha.AbstractURLSelector;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.topology.TopologyCheckAppConfig;
import org.apache.eagle.topology.TopologyEntityParserResult;
import org.apache.eagle.topology.extractor.TopologyExtractorBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class HdfsTopologyExtractor implements TopologyExtractorBase {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsTopologyExtractor.class);

    private HdfsTopologyEntityParser parser;
    private SpoutOutputCollector collector;
    private AbstractURLSelector urlSelector;

    public HdfsTopologyExtractor(TopologyCheckAppConfig config, SpoutOutputCollector collector) {
        this.urlSelector = new HdfsTopologyUrlSelector(config.hdfsConfig.namenodeUrls, AppConstants.CompressionType.GZIP);
        this.parser = new HdfsTopologyEntityParser(config.dataExtractorConfig.site, config.hdfsConfig);
        this.collector = collector;
    }

    private void checkUrl() throws IOException {
        if (!urlSelector.checkUrl()) {
            urlSelector.reSelectUrl();
        }
    }

    List<GenericMetricEntity> genericMetricEntityList(TopologyEntityParserResult entityParserResult) {
        return null;
    }


    @Override
    public void extract() {
        try {
            checkUrl();
        } catch (IOException e) {
            e.printStackTrace();
        }
        TopologyEntityParserResult result = parser.parse();
        if (result == null) {
            LOG.warn("No data fetched");
            return;
        }
        List<GenericMetricEntity> metrics = genericMetricEntityList(result);
        this.collector.emit(new Values(metrics));

    }
}
