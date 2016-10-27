/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.mr.history.parser;

import org.apache.eagle.jpm.mr.history.crawler.EagleOutputCollector;
import org.apache.eagle.jpm.mr.history.crawler.JobHistoryContentFilter;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class JHFParserFactory {

    private static final Logger LOG = LoggerFactory.getLogger(JHFParserFactory.class);

    public static JHFParserBase getParser(Map<String, String> baseTags,
                                          Configuration configuration,
                                          JobHistoryContentFilter filter,
                                          EagleOutputCollector outputCollector) {
        JHFMRVer2EventReader reader2 = new JHFMRVer2EventReader(baseTags, configuration, filter);
        reader2.addListener(new JobEntityCreationEagleServiceListener(outputCollector));
        reader2.addListener(new TaskFailureListener());
        reader2.addListener(new TaskAttemptCounterListener());
        reader2.addListener(new JobConfigurationCreationServiceListener());

        reader2.register(new JobEntityLifecycleAggregator());
        JHFParserBase parser = new JHFMRVer2Parser(reader2);
        return parser;
    }
}
