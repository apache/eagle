/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.stream.application.entity;


import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.stream.application.AppManagerConstants;
import org.apache.eagle.stream.application.model.TopologyDescriptionModel;
import org.apache.eagle.stream.application.model.TopologyOperationModel;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.HashMap;
import java.util.Map;


@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("eagle_metadata")
@ColumnFamily("f")
@Prefix("topologyDescription")
@Service(Constants.TOPOLOGY_DESCRIPTION_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(false)
@Tags({"topology"})
public class TopologyDescriptionEntity extends TaggedLogAPIEntity {
    @Column("a")
    private String exeClass;
    @Column("b")
    private String type;
    @Column("c")
    private String description;
    @Column("d")
    private String version;

    public String getExeClass() {
        return exeClass;
    }

    public void setExeClass(String exeClass) {
        this.exeClass = exeClass;
        valueChanged("exeClass");
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
        valueChanged("type");
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
        valueChanged("description");
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
        valueChanged("version");
    }

    public final static class TYPE {
        public final static String DYNAMIC = "DYNAMIC";
        public final static String CLASS = "CLASS";
    }

    public static TopologyDescriptionModel toModel(final TopologyDescriptionEntity entity){
        TopologyDescriptionModel model = new TopologyDescriptionModel(
                entity.getTags().get(AppManagerConstants.TOPOLOGY_TAG),
                entity.getExeClass(),
                null,
                entity.getType(),
                entity.getDescription(),
                entity.getVersion());
        return model;
    }

    public static TopologyDescriptionEntity fromModel(final TopologyDescriptionModel model){
        TopologyDescriptionEntity entity = new TopologyDescriptionEntity();
        Map<String,String> tags = new HashMap<String,String>(){{
            put(AppManagerConstants.TOPOLOGY_TAG, model.topology());
        }};
        entity.setExeClass(model.exeClass());
        entity.setDescription(model.description());
        entity.setType(model.topoType());
        entity.setVersion(model.version());
        entity.setTags(tags);
        return entity;
    }
}
