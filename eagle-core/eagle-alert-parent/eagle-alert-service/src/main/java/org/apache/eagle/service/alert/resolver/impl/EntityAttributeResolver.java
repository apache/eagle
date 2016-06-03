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
package org.apache.eagle.service.alert.resolver.impl;

import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.service.alert.resolver.AttributeResolvable;
import org.apache.eagle.service.alert.resolver.AttributeResolveException;
import org.apache.eagle.service.alert.resolver.BadAttributeResolveRequestException;
import org.apache.eagle.service.alert.resolver.GenericAttributeResolveRequest;
import org.apache.eagle.service.generic.GenericEntityServiceResource;
import org.apache.eagle.common.DateTimeUtil;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @since 6/16/15
 */
public class EntityAttributeResolver implements AttributeResolvable<EntityAttributeResolver.EntityAttributeResolveRequest,String> {

    private final static GenericEntityServiceResource entityServiceResource = new GenericEntityServiceResource();

    @Override
    public List<String> resolve(EntityAttributeResolveRequest request) throws AttributeResolveException {
        if(request.getFieldName()==null){
            throw new AttributeResolveException("fieldName is required");
        }
        String attributeName = request.getFieldName();
        EntityDefinition entityDefinition;
        try {
            if(request.getServiceName()!=null){
                entityDefinition = EntityDefinitionManager.getEntityByServiceName(request.getServiceName());
            }else if (request.getEntityClassName()!=null){
                Class<? extends TaggedLogAPIEntity> entityClass = (Class<? extends TaggedLogAPIEntity>) Class.forName(request.getEntityClassName());
                entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(entityClass);
            }else {
                throw new AttributeResolveException("At least serviceName or entityClassName is required, but neither found");
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new AttributeResolveException(e);
        }
        List<String> filterCondition = new ArrayList<>();
        if(request.getTags()!=null){
            for(Map.Entry<String,String> tag:request.getTags().entrySet()) {
                filterCondition.add("@" + tag.getKey() + " = \"" + tag.getValue() + "\"");
            }
        }
        if(request.getQuery() != null) {
            filterCondition.add("@" + attributeName + "~= \".*" + request.getQuery()+".*\"");
        }
        String query = entityDefinition.getService() + "[" + StringUtils.join(filterCondition, " AND ") + "]<@" + attributeName + ">{count}";
        return aggregateQuery(query, DateTimeUtil.millisecondsToHumanDateWithSeconds(0), DateTimeUtil.millisecondsToHumanDateWithSeconds(System.currentTimeMillis()),request.getMetricName());
    }

    @Override
    public Class<EntityAttributeResolveRequest> getRequestClass() {
        return EntityAttributeResolveRequest.class;
    }

    @Override
    public void validateRequest(EntityAttributeResolver.EntityAttributeResolveRequest request) throws BadAttributeResolveRequestException {

    }

    private List<String> aggregateQuery(String query,String startTime,String endTime,String metricName) throws AttributeResolveException {
        List<String> result = new ArrayList<>();
        GenericServiceAPIResponseEntity response = entityServiceResource.search(query, startTime, endTime, Integer.MAX_VALUE, null, false, false, 0, Integer.MAX_VALUE, true, 0, metricName, false);
        if(response.isSuccess()){
            List objs = response.getObj();
            for(Object item:objs){
                // TODO: get keys as result
                throw new IllegalArgumentException("not implemented yet");
            }
        }else{
            throw new AttributeResolveException(response.getException());
        }
        return result;
    }

    public static class EntityAttributeResolveRequest extends GenericAttributeResolveRequest {
        public Map<String, String> getTags() {
            return tags;
        }
        private final Map<String, String> tags;
        public String getMetricName() {
            return metricName;
        }
        private final String metricName;
        @JsonCreator
        public EntityAttributeResolveRequest(
                @JsonProperty("query") String query,
                @JsonProperty("site") String site,
                @JsonProperty("serviceName") String serviceName,
                @JsonProperty("entityClassName") String entityClassName,
                @JsonProperty("metricName") String metricName,
                @JsonProperty("fieldName") String fieldName,
                @JsonProperty("tags") Map<String, String> tags
        ){
            super(query, site);
            this.serviceName = serviceName;
            this.entityClassName = entityClassName;
            this.fieldName = fieldName;
            this.metricName = metricName;
            this.tags =  tags;
        }

        private final String serviceName;
        public String getEntityClassName() {
            return entityClassName;
        }
        public String getServiceName() {
            return serviceName;
        }
        public String getFieldName() {
            return fieldName;
        }
        private final String entityClassName;
        private final String fieldName;
    }
}