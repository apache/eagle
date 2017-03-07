/*
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

package org.apache.eagle.jpm.analyzer.publisher.dedup.impl;

import org.apache.eagle.jpm.analyzer.meta.model.AnalyzerEntity;
import org.apache.eagle.jpm.analyzer.publisher.Result;
import org.apache.eagle.jpm.analyzer.publisher.dedup.AlertDeduplicator;
import org.apache.eagle.jpm.analyzer.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * dedup by jobDefId.
 */
public class SimpleDeduplicator implements AlertDeduplicator, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleDeduplicator.class);

    private static Map<String, Long> lastUpdateTime = new HashMap<>();

    @Override
    public boolean dedup(AnalyzerEntity analyzerJobEntity, Result result) {
        synchronized (lastUpdateTime) {
            if (analyzerJobEntity.getJobMeta() == null || analyzerJobEntity.getJobMeta().getConfiguration() == null) {
                return false;
            }

            long dedupInterval = Constants.DEFAULT_DEDUP_INTERVAL;
            if (analyzerJobEntity.getJobMeta().getConfiguration().containsKey(Constants.DEDUP_INTERVAL_KEY)) {
                dedupInterval = (Integer)analyzerJobEntity.getJobMeta().getConfiguration().get(Constants.DEDUP_INTERVAL_KEY);
            }

            dedupInterval = dedupInterval * 1000;
            long currentTimeStamp = System.currentTimeMillis();
            if (lastUpdateTime.containsKey(analyzerJobEntity.getJobDefId())) {
                if (lastUpdateTime.get(analyzerJobEntity.getJobDefId()) + dedupInterval > currentTimeStamp) {
                    return true;
                } else {
                    lastUpdateTime.put(analyzerJobEntity.getJobDefId(), currentTimeStamp);
                    return false;
                }
            } else {
                lastUpdateTime.put(analyzerJobEntity.getJobDefId(), currentTimeStamp);
                return false;
            }
        }
    }
}
