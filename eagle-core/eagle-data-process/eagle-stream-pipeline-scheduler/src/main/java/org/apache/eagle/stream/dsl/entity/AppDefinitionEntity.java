/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.stream.dsl.entity;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;


@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Table("appDefinition")
@ColumnFamily("f")
@Prefix("appDefinition")
@Service("AppDefinitionService")
@TimeSeries(false)
@Tags({"site", "name"})
public class AppDefinitionEntity extends TaggedLogAPIEntity {

    /**
     * Definition code
     */
    @Column("a")
    private String definition;
    @Column("b")
    private String configuration;
    @Column("c")
    private String description;
    @Column("d")
    private String creator;
    @Column("e")
    private String executionStatus;
    @Column("f")
    private String executionEnvironment;
    @Column("g")
    private long updateTimestamp;
    @Column("h")
    private long createTimestamp;

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setUpdateTimestamp(long updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
        valueChanged("updateTimestamp");
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public void setCreateTimestamp(long createTimestamp) {
        this.createTimestamp = createTimestamp;
        valueChanged("createTimestamp");
    }

    public String getDefinition() {
        return definition;
    }

    public void setDefinition(String definition) {
        this.definition = definition;
        valueChanged("definition");
    }

    public String getConfiguration() {
        return configuration;
    }

    public void setConfiguration(String configuration) {
        this.configuration = configuration;
        valueChanged("configuration");
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
        valueChanged("description");
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
        valueChanged("creator");
    }

    public String getExecutionStatus() {
        return executionStatus;
    }

    public void setExecutionStatus(String executionStatus) {
        this.executionStatus = executionStatus;
        valueChanged("executionStatus");
    }

    public String getExecutionEnvironment() {
        return executionEnvironment;
    }

    public void setExecutionEnvironment(String executionEnvironment) {
        this.executionEnvironment = executionEnvironment;
        valueChanged("executionEnvironment");
    }

    public final static class STATUS {
        public final static String UNKNOWN = "UNKNOWN";
        public final static String INITIALIZED = "INITIALIZED";
        public final static String RUNNING = "RUNNING";
        public final static String STARTING = "STARTING";
        public final static String STOPPING = "STOPPING";
        public final static String STOPPED = "STOPPED";
    }

    public void validate() throws Exception {
        if(definition == null) throw new IllegalArgumentException("definition should not empty");
        // new StreamAppEvaluator(definition, ConfigFactory.parseMap(environment)).compile();
    }
}