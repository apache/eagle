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
package org.apache.eagle.alert.engine.coordinator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.*;

/**
 * @since Apr 5, 2016
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PolicyDefinition implements Serializable{
    private static final long serialVersionUID = 377581499339572414L;
    // unique identifier
    private String name;
    private String description;
    private List<String> inputStreams = new ArrayList<String>();
    private List<String> outputStreams = new ArrayList<String>();

    private Definition definition;
    private Definition stateDefinition;
    private PolicyStatus policyStatus = PolicyStatus.ENABLED;

    // one stream only have one partition in one policy, since we don't support stream alias
    private List<StreamPartition> partitionSpec = new ArrayList<StreamPartition>();

    // runtime configuration for policy, these are user-invisible
    private int parallelismHint = 1;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<String> getInputStreams() {
        return inputStreams;
    }

    public void setInputStreams(List<String> inputStreams) {
        this.inputStreams = inputStreams;
    }

    public List<String> getOutputStreams() {
        return outputStreams;
    }

    public void setOutputStreams(List<String> outputStreams) {
        this.outputStreams = outputStreams;
    }

    public Definition getDefinition() {
        return definition;
    }

    public Definition getStateDefinition() {
        return stateDefinition;
    }

    public void setStateDefinition(Definition stateDefinition) {
        this.stateDefinition = stateDefinition;
    }

    public void setDefinition(Definition definition) {
        this.definition = definition;
    }

    public List<StreamPartition> getPartitionSpec() {
        return partitionSpec;
    }

    public void setPartitionSpec(List<StreamPartition> partitionSpec) {
        this.partitionSpec = partitionSpec;
    }

    public void addPartition(StreamPartition par) {
        this.partitionSpec.add(par);
    }

    public int getParallelismHint() {
        return parallelismHint;
    }

    public void setParallelismHint(int parallelism) {
        this.parallelismHint = parallelism;
    }

    public PolicyStatus getPolicyStatus() {
		return policyStatus;
	}

	public void setPolicyStatus(PolicyStatus policyStatus) {
		this.policyStatus = policyStatus;
	}

	@Override
    public int hashCode() {
        return new HashCodeBuilder().
                append(name).
                append(inputStreams).
                append(outputStreams).
                append(definition).
                append(partitionSpec).
//                append(parallelismHint).
                build();
    }

    @Override
    public boolean equals(Object that){
        if(that == this)
            return true;
        if(! (that instanceof PolicyDefinition))
            return false;
        PolicyDefinition another = (PolicyDefinition)that;
        if(Objects.equals(another.name, this.name) &&
        		Objects.equals(another.description, this.description) &&
                CollectionUtils.isEqualCollection(another.inputStreams, this.inputStreams) &&
                CollectionUtils.isEqualCollection(another.outputStreams, this.outputStreams) &&
                another.definition.equals(this.definition) &&
                Objects.equals(this.definition, another.definition) &&
                CollectionUtils.isEqualCollection(another.partitionSpec, this.partitionSpec) 
//                && another.parallelismHint == this.parallelismHint
                ) {
            return true;
        }
        return false;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Definition implements Serializable{
        private static final long serialVersionUID = -622366527887848346L;

        public String type;
        public String value;
        public String handlerClass;
        public Map<String, Object> properties = new HashMap<>();

        private List<String> inputStreams = new ArrayList<String>();
        private List<String> outputStreams = new ArrayList<String>();

        public Definition(String type,String value){
            this.type = type;
            this.value = value;
        }

        public Definition() {
            this.type = null;
            this.value = null;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(type).append(value).build();
        }

        @Override
        public boolean equals(Object that){
            if(that == this)
                return true;
            if(!(that instanceof Definition))
                return false;
            Definition another = (Definition)that;
            if(another.type.equals(this.type)
                    && another.value.equals(this.value)
                    && ListUtils.isEqualList(another.inputStreams, this.inputStreams)
                    && ListUtils.isEqualList(another.outputStreams, this.outputStreams))
                return true;
            return false;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public void setInputStreams(List<String> inputStreams) {
            this.inputStreams = inputStreams;
        }

        public void setOutputStreams(List<String> outputStreams) {
            this.outputStreams = outputStreams;
        }

        public List<String> getInputStreams() {
            return inputStreams;
        }

        public List<String> getOutputStreams() {
            return outputStreams;
        }

        public String getHandlerClass() {
            return handlerClass;
        }

        public void setHandlerClass(String handlerClass) {
            this.handlerClass = handlerClass;
        }

        public Map<String, Object> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, Object> properties) {
            this.properties = properties;
        }

        @Override
        public String toString() {
            return String.format("{type=\"%s\",value=\"%s\", inputStreams=\"%s\", outputStreams=\"%s\" }",type,value, inputStreams, outputStreams);
        }
    }
    
    public static enum PolicyStatus {
    	ENABLED, DISABLED
    }

    @Override
    public String toString() {
        return String.format("{name=\"%s\",definition=%s}",this.getName(),this.getDefinition()==null?"null": this.getDefinition().toString());
    }
}
