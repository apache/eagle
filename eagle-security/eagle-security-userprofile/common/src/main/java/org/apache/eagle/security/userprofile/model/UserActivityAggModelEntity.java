/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.security.userprofile.model;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.security.userprofile.UserProfileConstants;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.eagle.log.entity.meta.*;
import org.apache.eagle.security.userprofile.UserProfileConstants;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import scala.collection.JavaConversions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("userprofile")
@ColumnFamily("f")
@Prefix("useractivity")
@Service(UserProfileConstants.USER_ACTIVITY_AGG_MODEL_SERVICE)
@TimeSeries(true)
@JsonIgnoreProperties(ignoreUnknown = true)
@Tags({"site","user"})
public class UserActivityAggModelEntity extends TaggedLogAPIEntity{
    @Column("a")
    private List<String> cmdTypes;
    @Column("b")
    private double[][] cmdMatrix;

    public double[][] getCmdMatrix() {
        return cmdMatrix;
    }

    public void setCmdMatrix(double[][] cmdMatrix) {
        this.cmdMatrix = cmdMatrix;
        valueChanged("cmdMatrix");
    }

    public List<String> getCmdTypes() {
        return cmdTypes;
    }

    public void setCmdTypes(List<String> cmdTypes) {
        this.cmdTypes = cmdTypes;
        valueChanged("cmdTypes");
    }

    public static UserActivityAggModel toModel(final UserActivityAggModelEntity entity){
        //return null;
        UserActivityAggModel model = new UserActivityAggModel(
                entity.getTags().get(UserProfileConstants.USER_TAG),
                new Array2DRowRealMatrix(entity.getCmdMatrix()),
                scala.collection.JavaConversions.asScalaBuffer(entity.getCmdTypes()),
                entity.getTags().get(UserProfileConstants.SITE_TAG),
                entity.getTimestamp());
        return model;
    }

    public static UserActivityAggModelEntity fromModel(final UserActivityAggModel model){
        UserActivityAggModelEntity entity = new UserActivityAggModelEntity();
        entity.setCmdTypes(JavaConversions.asJavaList(model.cmdTypes()));
        entity.setCmdMatrix(model.matrix().getData());
        Map<String,String> tags = new HashMap<String,String>(){{
            put(UserProfileConstants.SITE_TAG,model.site());
            put(UserProfileConstants.USER_TAG,model.user());
        }};
        entity.setTimestamp(model.timestamp());
        entity.setTags(tags);
        return entity;
    }
}