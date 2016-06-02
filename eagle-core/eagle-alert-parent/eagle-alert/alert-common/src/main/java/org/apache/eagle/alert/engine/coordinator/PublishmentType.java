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

package org.apache.eagle.alert.engine.coordinator;

import java.util.Objects;

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class PublishmentType {

    private String type;
    private String className;
    private String description;
    private String fields;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getClassName(){
        return className;
    }
    public void setClassName(String className){
        this.className = className;
    }

    public String getDescription(){
        return description;
    }
    public void setDescription(String description){
        this.description = description;
    }

    public String getFields() {
        return fields;
    }

    public void setFields(String fields) {
        this.fields = fields;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PublishmentType) {
            PublishmentType p = (PublishmentType) obj;
            return (Objects.equals(className, p.getClassName()) &&
                    Objects.equals(type, p.type) && 
                    Objects.equals(description, p.getDescription()) &&
                    Objects.equals(fields, p.getFields()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(className)
                .append(type)
                .append(description)
                .append(fields)
                .build();
    }
}
