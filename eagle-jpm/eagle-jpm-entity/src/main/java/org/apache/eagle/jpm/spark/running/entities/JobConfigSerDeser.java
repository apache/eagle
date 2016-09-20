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

package org.apache.eagle.jpm.spark.running.entities;

import org.apache.eagle.log.entity.meta.EntitySerDeser;
import org.apache.eagle.log.entity.meta.MapSerDeser;

import java.util.Map;

/**
 * refactor this class later.
 */
public class JobConfigSerDeser implements EntitySerDeser<JobConfig> {
    private static final MapSerDeser INSTANCE = new MapSerDeser();

    @Override
    public JobConfig deserialize(byte[] bytes) {
        Map map = INSTANCE.deserialize(bytes);
        JobConfig config = new JobConfig();
        config.putAll(map);
        return config;
    }

    @Override
    public byte[] serialize(JobConfig jobConfig) {
        return INSTANCE.serialize(jobConfig);
    }

    @Override
    public Class<JobConfig> type() {
        return JobConfig.class;
    }
}