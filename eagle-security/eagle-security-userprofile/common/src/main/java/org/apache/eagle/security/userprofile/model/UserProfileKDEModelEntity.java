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
import org.apache.eagle.ml.model.MLModelAPIEntity;
import org.apache.eagle.security.userprofile.UserProfileConstants;

import org.apache.eagle.security.userprofile.UserProfileConstants;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class UserProfileKDEModelEntity {
    public static UserProfileKDEModel deserializeModel(MLModelAPIEntity entity) throws IOException {
        if(entity == null) return null;
        String user = entity.getTags()==null? null:entity.getTags().get(UserProfileConstants.USER_TAG);
        String site = entity.getTags()==null? null:entity.getTags().get(UserProfileConstants.SITE_TAG);
        UserProfileKDEModelEntity content = TaggedLogAPIEntity.buildObjectMapper().readValue(entity.getContent(), UserProfileKDEModelEntity.class);

        UserCommandStatistics[] statistics = null;
        if(content.getStatistics()!=null) {
            statistics = new UserCommandStatistics[content.getStatistics().size()];
            int i = 0;
            for (Map<String, Object> map : content.getStatistics()) {
                statistics[i] = new UserCommandStatistics(map);
                i++;
            }
        }
        return new UserProfileKDEModel(
                entity.getVersion(), site,user,statistics,
                content.getMinProbabilityEstimate(), content.getMaxProbabilityEstimate(),
                content.getNintyFivePercentileEstimate(), content.getMedianProbabilityEstimate());
    }

    public static MLModelAPIEntity serializeModel(UserProfileKDEModel model) throws IOException {
        if(model == null) return null;
        MLModelAPIEntity entity = new MLModelAPIEntity();
        UserProfileKDEModelEntity content = new UserProfileKDEModelEntity();
        entity.setVersion(model.version());
        if(model.statistics()!=null){
            List<Map<String,Object>> statisticsMaps = new ArrayList<>(model.statistics().length);
            for(UserCommandStatistics userCommandStatistics :model.statistics()){
                statisticsMaps.add(userCommandStatistics.toMap());
            }
            content.setStatistics(statisticsMaps);
        }
        content.setMinProbabilityEstimate(model.minProbabilityEstimate());
        content.setMaxProbabilityEstimate(model.maxProbabilityEstimate());
        content.setNintyFivePercentileEstimate(model.nintyFivePercentileEstimate());
        content.setMedianProbabilityEstimate(model.medianProbabilityEstimate());

        Map<String,String> tags = new HashMap<>();
        tags.put(UserProfileConstants.SITE_TAG,model.site());
        tags.put(UserProfileConstants.USER_TAG,model.user());
        tags.put(UserProfileConstants.ALGORITHM_TAG,model.algorithm());

        entity.setTags(tags);
        entity.setContent(TaggedLogAPIEntity.buildObjectMapper().writeValueAsString(content));
        return entity;
    }

    private List<Map<String,Object>> statistics;
    private double minProbabilityEstimate;
    private double maxProbabilityEstimate;
    private double nintyFivePercentileEstimate;
    private double medianProbabilityEstimate;

    public double getMedianProbabilityEstimate() {
        return medianProbabilityEstimate;
    }

    public void setMedianProbabilityEstimate(double medianProbabilityEstimate) {
        this.medianProbabilityEstimate = medianProbabilityEstimate;
    }

    public List<Map<String,Object>> getStatistics() {
        return statistics;
    }

    public void setStatistics(List<Map<String,Object>> statistics) {
        this.statistics = statistics;
    }

    public double getMinProbabilityEstimate() {
        return minProbabilityEstimate;
    }

    public void setMinProbabilityEstimate(double minProbabilityEstimate) {
        this.minProbabilityEstimate = minProbabilityEstimate;
    }

    public double getMaxProbabilityEstimate() {
        return maxProbabilityEstimate;
    }

    public void setMaxProbabilityEstimate(double maxProbabilityEstimate) {
        this.maxProbabilityEstimate = maxProbabilityEstimate;
    }

    public double getNintyFivePercentileEstimate() {
        return nintyFivePercentileEstimate;
    }

    public void setNintyFivePercentileEstimate(double nintyFivePercentileEstimate) {
        this.nintyFivePercentileEstimate = nintyFivePercentileEstimate;
    }
}