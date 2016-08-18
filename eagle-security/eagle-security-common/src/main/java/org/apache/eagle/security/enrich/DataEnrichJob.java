/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.security.enrich;

import com.google.common.collect.Maps;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * Since 8/16/16.
 */
public class DataEnrichJob implements Job {
    private final static Logger LOG = LoggerFactory.getLogger(DataEnrichJob.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();

        DataEnrichLCM lcm = (DataEnrichLCM)jobDataMap.getOrDefault("dataEnrichLCM", null);
        if(lcm == null)
            throw new IllegalStateException("dataEnrichLCM implementation should be provided");
        try {
            Collection externalEntities = lcm.loadExternal();
            Map<Object, Object> map = Maps.uniqueIndex(
                    externalEntities,
                    entity -> lcm.getCacheKey(entity)
                );
            ExternalDataCache.getInstance().setJobResult(lcm.getClass(), map);
        } catch(Exception ex) {
            LOG.error("Fail to load hbase resource sensitivity data", ex);
        }
    }
}
