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

package org.apache.eagle.jpm.mr.history.crawler;

import org.apache.eagle.jpm.mr.history.MRHistoryJobConfig;
import org.apache.eagle.jpm.mr.history.parser.JHFParserBase;
import org.apache.eagle.jpm.mr.history.parser.JHFParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class DefaultJHFInputStreamCallback implements JHFInputStreamCallback {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultJHFInputStreamCallback.class);


    private JobHistoryContentFilter filter;

    public DefaultJHFInputStreamCallback(JobHistoryContentFilter filter, EagleOutputCollector eagleCollector) {
        this.filter = filter;
    }

    @Override
    public void onInputStream(InputStream jobFileInputStream, org.apache.hadoop.conf.Configuration conf) throws Exception {
        @SuppressWarnings("serial")
        Map<String, String> baseTags = new HashMap<String, String>() {
            {
                put("site", MRHistoryJobConfig.get().getJobExtractorConfig().site);
            }
        };

        if (!filter.acceptJobFile()) {
            // close immediately if we don't need job file
            jobFileInputStream.close();
        } else {
            //get parser and parse, do not need to emit data now
            JHFParserBase parser = JHFParserFactory.getParser(baseTags, conf, filter);
            parser.parse(jobFileInputStream);
            jobFileInputStream.close();
        }
    }
}
