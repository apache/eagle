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
package org.apache.eagle.alert.coordination.model;

import com.google.common.base.Objects;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.Map;

/**
 * This metadata model controls how to convert kafka topic into tuple stream.
 * @since Apr 5, 2016
 */
public class Kafka2TupleMetadata  implements Serializable {
    private String type;
    private String name; // data source name
    private Map<String, String> properties;
    private String topic;
    private String schemeCls;

    private Tuple2StreamMetadata codec;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setSchemeCls(String schemeCls) {
        this.schemeCls = schemeCls;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Tuple2StreamMetadata getCodec() {
        return codec;
    }

    public void setCodec(Tuple2StreamMetadata codec) {
        this.codec = codec;
    }

    public String getTopic() {
        return this.topic;
    }

    public String getSchemeCls() {
        return this.schemeCls;
    }

    public int hashCode() {
        return new HashCodeBuilder().append(name).append(type).build();
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof Kafka2TupleMetadata)) {
            return false;
        }
        Kafka2TupleMetadata o = (Kafka2TupleMetadata) obj;
        return Objects.equal(name, o.name) && Objects.equal(type, o.type);
    }

}
