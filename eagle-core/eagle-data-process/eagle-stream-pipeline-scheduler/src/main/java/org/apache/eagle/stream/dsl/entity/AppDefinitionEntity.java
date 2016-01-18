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

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.apache.eagle.stream.dsl.AppConstants;
import org.apache.eagle.stream.dsl.execution.model.StreamAppDefinition;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import scala.collection.JavaConversions;

import java.util.HashMap;
import java.util.Map;


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
    private String executionCluster;
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

    public String getExecutionCluster() {
        return executionCluster;
    }

    public void setExecutionCluster(String executionCluster) {
        this.executionCluster = executionCluster;
        valueChanged("executionCluster");
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

    public static StreamAppDefinition toModel(final AppDefinitionEntity entity){
        StreamAppDefinition model = new StreamAppDefinition(
                entity.getTags().get(AppConstants.SITE_TAG),
                entity.getTags().get(AppConstants.APP_NAME_TAG),
                entity.getDefinition(),
                entity.getConfiguration(),
                entity.getDescription(),
                entity.getCreator(),
                entity.getExecutionStatus(),
                entity.getExecutionCluster(),
                entity.getUpdateTimestamp(),
                entity.getCreateTimestamp());
        return model;
    }

    public static AppDefinitionEntity fromModel(final StreamAppDefinition model){
        AppDefinitionEntity entity = new AppDefinitionEntity();
        entity.setDefinition(model.definition());
        entity.setConfiguration(model.configuration());
        entity.setDescription(model.description());
        entity.setCreator(model.creator());
        entity.setExecutionCluster(model.executionCluster());
        entity.setExecutionStatus(model.executionStatus());
        Map<String,String> tags = new HashMap<String,String>(){{
            put(AppConstants.SITE_TAG, model.site());
            put(AppConstants.APP_NAME_TAG, model.name());
        }};
        entity.setUpdateTimestamp(model.updateTimestamp());
        entity.setCreateTimestamp(model.createTimestamp());
        entity.setTags(tags);
        return entity;
    }
}