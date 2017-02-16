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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.*;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PublishmentType {
    private String name;

    @Override
    public String toString() {
        return "PublishmentType{"
                + "name='" + name + '\''
                + ", type='" + type + '\''
                + ", description='" + description + '\''
                + ", fields=" + fields
                + '}';
    }

    private String type;
    private String description;
    private List<Map<String, String>> fields = new LinkedList<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<Map<String, String>> getFields() {
        return fields;
    }

    public void setFields(List<Map<String, String>> fields) {
        this.fields = fields;
    }



    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PublishmentType) {
            PublishmentType p = (PublishmentType) obj;
            return (Objects.equals(name, p.name)
                && Objects.equals(type, p.type)
                && Objects.equals(description, p.getDescription())
                && Objects.equals(fields, p.getFields()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
            .append(name)
            .append(type)
            .append(description)
            .append(fields)
            .build();
    }


    public static class Builder {
        private final PublishmentType publishmentType;

        public Builder() {
            this.publishmentType = new PublishmentType();
        }

        public Builder type(Class<?> typeClass) {
            this.publishmentType.setType(typeClass.getName());
            return this;
        }

        public Builder name(String name) {
            this.publishmentType.setName(name);
            return this;
        }

        public Builder description(String description) {
            this.publishmentType.setDescription(description);
            return this;
        }

        public Builder field(Map<String,String> fieldDesc) {
            this.publishmentType.getFields().add(fieldDesc);
            return this;
        }

        public Builder field(String name, String value) {
            this.publishmentType.getFields().add(new HashMap<String,String>() {
                {
                    put("name", name);
                    put("value", value);
                }
            });
            return this;
        }

        public Builder field(String name) {
            this.publishmentType.getFields().add(new HashMap<String,String>() {
                {
                    put("name", name);
                }
            });
            return this;
        }

        public PublishmentType build() {
            return this.publishmentType;
        }
    }
}