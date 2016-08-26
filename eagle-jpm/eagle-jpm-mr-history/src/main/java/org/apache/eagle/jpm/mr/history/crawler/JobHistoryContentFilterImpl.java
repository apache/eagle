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

import java.util.List;
import java.util.regex.Pattern;

public class JobHistoryContentFilterImpl implements JobHistoryContentFilter {
    private boolean acceptJobFile;
    private boolean acceptJobConfFile;
    private List<Pattern> mustHaveJobConfKeyPatterns;
    private List<Pattern> jobConfKeyInclusionPatterns;
    private List<Pattern> jobConfKeyExclusionPatterns;

    private String jobNameKey;

    @Override
    public boolean acceptJobFile() {
        return acceptJobFile;
    }

    @Override
    public boolean acceptJobConfFile() {
        return acceptJobConfFile;
    }

    @Override
    public List<Pattern> getMustHaveJobConfKeyPatterns() {
        return mustHaveJobConfKeyPatterns;
    }

    @Override
    public List<Pattern> getJobConfKeyInclusionPatterns() {
        return jobConfKeyInclusionPatterns;
    }

    @Override
    public List<Pattern> getJobConfKeyExclusionPatterns() {
        return jobConfKeyExclusionPatterns;
    }

    @Override
    public String getJobNameKey() {
        return jobNameKey;
    }

    public void setJobNameKey(String jobNameKey) {
        this.jobNameKey = jobNameKey;
    }

    public void setAcceptJobFile(boolean acceptJobFile) {
        this.acceptJobFile = acceptJobFile;
    }

    public void setAcceptJobConfFile(boolean acceptJobConfFile) {
        this.acceptJobConfFile = acceptJobConfFile;
    }

    public void setJobConfKeyInclusionPatterns(
            List<Pattern> jobConfKeyInclusionPatterns) {
        this.jobConfKeyInclusionPatterns = jobConfKeyInclusionPatterns;
    }

    public void setJobConfKeyExclusionPatterns(
            List<Pattern> jobConfKeyExclusionPatterns) {
        this.jobConfKeyExclusionPatterns = jobConfKeyExclusionPatterns;
    }

    public void setMustHaveJobConfKeyPatterns(List<Pattern> mustHaveJobConfKeyPatterns) {
        this.mustHaveJobConfKeyPatterns = mustHaveJobConfKeyPatterns;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("job history file:");
        sb.append(acceptJobFile);
        sb.append(", job config file:");
        sb.append(acceptJobConfFile);
        if (acceptJobConfFile) {
            sb.append(", must contain keys:");
            sb.append(mustHaveJobConfKeyPatterns);
            sb.append(", include keys:");
            sb.append(jobConfKeyInclusionPatterns);
            sb.append(", exclude keys:");
            sb.append(jobConfKeyExclusionPatterns);
        }
        return sb.toString();
    }
}
