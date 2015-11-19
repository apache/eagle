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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.eagle.security.hbase.sensitivity;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import org.apache.eagle.security.hbase.HbaseResourceSensitivityAPIEntity;
import org.apache.eagle.security.util.AbstractResourceSensitivityPollingJob;
import org.apache.eagle.security.util.ExternalDataCache;
import org.apache.eagle.security.hbase.HbaseResourceSensitivityAPIEntity;
import org.apache.eagle.security.util.AbstractResourceSensitivityPollingJob;
import org.apache.eagle.security.util.ExternalDataCache;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class HbaseResourceSensitivityPollingJob extends AbstractResourceSensitivityPollingJob implements Job {
    private final static Logger LOG = LoggerFactory.getLogger(HbaseResourceSensitivityPollingJob.class);

    @Override
    public void execute(JobExecutionContext context)
            throws JobExecutionException {
        JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
        try {
            List<HbaseResourceSensitivityAPIEntity>
            hbaseResourceSensitivity = load(jobDataMap, "HbaseResourceSensitivityService");
            if(hbaseResourceSensitivity == null) {
            	LOG.warn("Hbase resource sensitivity information is empty");
            	return;
            }
            Map<String, HbaseResourceSensitivityAPIEntity> map = Maps.uniqueIndex(
            		hbaseResourceSensitivity,
            		new Function<HbaseResourceSensitivityAPIEntity, String>() {
            			@Override
            			public String apply(HbaseResourceSensitivityAPIEntity input) {
            				return input.getTags().get("hbaseResource");
            			}
            		});
            ExternalDataCache.getInstance().setJobResult(getClass(), map);
        } catch(Exception ex) {
        	LOG.error("Fail to load hbase resource sensitivity data", ex);
        }
    }

}