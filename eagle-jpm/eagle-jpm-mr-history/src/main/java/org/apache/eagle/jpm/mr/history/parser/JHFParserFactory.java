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

import org.apache.eagle.jpm.mr.history.common.JHFConfigManager;
import org.apache.eagle.jpm.mr.history.crawler.JobHistoryContentFilter;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class JHFParserFactory {

    private static final Logger LOG = LoggerFactory.getLogger(JHFParserFactory.class);

    public static JHFParserBase getParser(JHFConfigManager configManager, Map<String, String> baseTags, Configuration configuration, JobHistoryContentFilter filter) {
        String format = configManager.getJobExtractorConfig().mrVersion;
        JHFParserBase parser;
        JHFFormat f;
        try {
            if (format == null) {
                f = JHFFormat.MRVer1;
            } else {
                f = JHFFormat.valueOf(format);
            }
        } catch(IllegalArgumentException ex) {
            f = JHFFormat.MRVer1; // fall back to version 1 unless it's specified as version 2
        }
        
        switch (f) {
        case MRVer2:
            JHFMRVer2EventReader reader2 = new JHFMRVer2EventReader(baseTags, configuration, filter);
            reader2.addListener(new JobEntityCreationEagleServiceListener(configManager));
            reader2.addListener(new TaskFailureListener(configManager));
            reader2.addListener(new TaskAttemptCounterListener(configManager));
            reader2.addListener(new JobConfigurationCreationServiceListener(configManager));

            reader2.register(new JobEntityLifecycleAggregator());
            parser = new JHFMRVer2Parser(reader2);
            break;
        case MRVer1:
        default:
            JHFMRVer1EventReader reader1 = new JHFMRVer1EventReader(baseTags, configuration, filter);
            reader1.addListener(new JobEntityCreationEagleServiceListener(configManager));
            reader1.addListener(new TaskFailureListener(configManager));
            reader1.addListener(new TaskAttemptCounterListener(configManager));

            reader1.register(new JobEntityLifecycleAggregator());
            parser = new JHFMRVer1Parser(reader1);
            break;
        }
        return parser;
    }
}
