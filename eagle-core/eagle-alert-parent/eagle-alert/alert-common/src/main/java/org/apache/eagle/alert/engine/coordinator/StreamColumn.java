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

import java.io.Serializable;


public class StreamColumn implements Serializable {
    private static final long serialVersionUID = -5457861313624389106L;
    private String name;
    private Type type;
    private Object defaultValue;
    private boolean required;
    private String description;

    public String toString() {
        return String.format("StreamColumn=name[%s], type=[%s], defaultValue=[%s], required=[%s]", name, type,
                defaultValue, required);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public enum Type implements Serializable {
        STRING("string"), INT("int"), LONG("long"), FLOAT("float"), DOUBLE("double"), BOOL("bool"), OBJECT("object");

        private final String name;

        Type(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        @org.codehaus.jackson.annotate.JsonCreator
        @com.fasterxml.jackson.annotation.JsonCreator
        public static Type getEnumFromValue(String value) {
            for (Type testEnum : values()) {
                if (testEnum.name.equalsIgnoreCase(value)) {
                    return testEnum;
                }
            }
            throw new IllegalArgumentException();
        }
    }

    public static class Builder {
        private StreamColumn column;

        public Builder() {
            column = new StreamColumn();
        }

        public Builder name(String name) {
            column.setName(name);
            return this;
        }

        public Builder type(Type type) {
            column.setType(type);
            return this;
        }

        public Builder defaultValue(Object defaultValue) {
            column.setDefaultValue(defaultValue);
            return this;
        }

        public Builder required(boolean required) {
            column.setRequired(required);
            return this;
        }

        public StreamColumn build() {
            return column;
        }
    }
}