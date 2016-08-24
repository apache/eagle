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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class JobHistoryContentFilterBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(JobHistoryContentFilterBuilder.class);

    private boolean acceptJobFile;
    private boolean acceptJobConfFile;
    private List<Pattern> mustHaveJobConfKeyPatterns;
    private List<Pattern> jobConfKeyInclusionPatterns;
    private List<Pattern> jobConfKeyExclusionPatterns;

    private String jobNameKey;

    public static JobHistoryContentFilterBuilder newBuilder() {
        return new JobHistoryContentFilterBuilder();
    }

    public JobHistoryContentFilterBuilder acceptJobFile() {
        this.acceptJobFile = true;
        return this;
    }

    public JobHistoryContentFilterBuilder acceptJobConfFile() {
        this.acceptJobConfFile = true;
        return this;
    }

    public JobHistoryContentFilterBuilder mustHaveJobConfKeyPatterns(Pattern ...patterns) {
        mustHaveJobConfKeyPatterns = Arrays.asList(patterns);
        if (jobConfKeyInclusionPatterns != null) {
            List<Pattern> list = new ArrayList<Pattern>();
            list.addAll(jobConfKeyInclusionPatterns);
            list.addAll(Arrays.asList(patterns));
            jobConfKeyInclusionPatterns = list;
        } else {
            jobConfKeyInclusionPatterns = Arrays.asList(patterns);
        }
        return this;
    }

    public JobHistoryContentFilterBuilder includeJobKeyPatterns(Pattern ... patterns) {
        if (jobConfKeyInclusionPatterns != null) {
            List<Pattern> list = new ArrayList<Pattern>();
            list.addAll(jobConfKeyInclusionPatterns);
            list.addAll(Arrays.asList(patterns));
            jobConfKeyInclusionPatterns = list;
        } else {
            jobConfKeyInclusionPatterns = Arrays.asList(patterns);
        }
        return this;
    }

    public JobHistoryContentFilterBuilder excludeJobKeyPatterns(Pattern ...patterns) {
        jobConfKeyExclusionPatterns = Arrays.asList(patterns);
        return this;
    }

    public JobHistoryContentFilterBuilder setJobNameKey(String jobNameKey) {
        this.jobNameKey = jobNameKey;
        return this;
    }

    public JobHistoryContentFilter build() {
        JobHistoryContentFilterImpl filter = new JobHistoryContentFilterImpl();
        filter.setAcceptJobFile(acceptJobFile);
        filter.setAcceptJobConfFile(acceptJobConfFile);
        filter.setMustHaveJobConfKeyPatterns(mustHaveJobConfKeyPatterns);
        filter.setJobConfKeyInclusionPatterns(jobConfKeyInclusionPatterns);
        filter.setJobConfKeyExclusionPatterns(jobConfKeyExclusionPatterns);
        filter.setJobNameKey(jobNameKey);
        LOG.info("job history content filter:" + filter);
        return filter;
    }
}
