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
    private boolean m_acceptJobFile;
    private boolean m_acceptJobConfFile;
    private List<Pattern> m_mustHaveJobConfKeyPatterns;
    private List<Pattern> m_jobConfKeyInclusionPatterns;
    private List<Pattern> m_jobConfKeyExclusionPatterns;

    @Override
    public boolean acceptJobFile() {
        return m_acceptJobFile;
    }

    @Override
    public boolean acceptJobConfFile() {
        return m_acceptJobConfFile;
    }

    @Override
    public List<Pattern> getMustHaveJobConfKeyPatterns() {
        return m_mustHaveJobConfKeyPatterns;
    }

    @Override
    public List<Pattern> getJobConfKeyInclusionPatterns() {
        return m_jobConfKeyInclusionPatterns;
    }

    @Override
    public List<Pattern> getJobConfKeyExclusionPatterns() {
        return m_jobConfKeyExclusionPatterns;
    }

    public void setAcceptJobFile(boolean acceptJobFile) {
        this.m_acceptJobFile = acceptJobFile;
    }

    public void setAcceptJobConfFile(boolean acceptJobConfFile) {
        this.m_acceptJobConfFile = acceptJobConfFile;
    }

    public void setJobConfKeyInclusionPatterns(
            List<Pattern> jobConfKeyInclusionPatterns) {
        this.m_jobConfKeyInclusionPatterns = jobConfKeyInclusionPatterns;
    }

    public void setJobConfKeyExclusionPatterns(
            List<Pattern> jobConfKeyExclusionPatterns) {
        this.m_jobConfKeyExclusionPatterns = jobConfKeyExclusionPatterns;
    }

    public void setMustHaveJobConfKeyPatterns(List<Pattern> mustHaveJobConfKeyPatterns) {
        this.m_mustHaveJobConfKeyPatterns = mustHaveJobConfKeyPatterns;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("job history file:");
        sb.append(m_acceptJobFile);
        sb.append(", job config file:");
        sb.append(m_acceptJobConfFile);
        if(m_acceptJobConfFile){
            sb.append(", must contain keys:");
            sb.append(m_mustHaveJobConfKeyPatterns);
            sb.append(", include keys:");
            sb.append(m_jobConfKeyInclusionPatterns);
            sb.append(", exclude keys:");
            sb.append(m_jobConfKeyExclusionPatterns);
        }
        return sb.toString();
    }
}
