/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.jpm.mr.historyentity;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;

import java.util.List;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import static org.apache.eagle.jpm.util.Constants.MR_JOB_OPTIMIZER_SUGGESTION_SERVICE_NAME;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Table("eaglejpa")
@ColumnFamily("f")
@Prefix("jsuggestion")
@Service(MR_JOB_OPTIMIZER_SUGGESTION_SERVICE_NAME)
@TimeSeries(true)
@Partition({"site"})
@Indexes({
        @Index(name = "Index_1_jobId", columns = { "jobId" }, unique = true),
        @Index(name = "Index_2_jobDefId", columns = { "jobDefId" }, unique = false)
        })
public class JobSuggestionAPIEntity extends TaggedLogAPIEntity {
    @Column("a")
    private String optimizerSuggestion;
    @Column("b")
    private List<String> optimizerSettings;

    public String getOptimizerSuggestion() {
        return optimizerSuggestion;
    }

    public void setOptimizerSuggestion(String optimizerSuggestion) {
        this.optimizerSuggestion = optimizerSuggestion;
        valueChanged("optimizerSuggestion");
    }

    public List<String> getOptimizerSettings() {
        return optimizerSettings;
    }

    public void setOptimizerSettings(List<String> optimizerSettings) {
        this.optimizerSettings = optimizerSettings;
        valueChanged("optimizerSettings");
    }

}
