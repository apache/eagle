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

import com.typesafe.config.ConfigFactory;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.Tags;
import org.apache.eagle.stream.dsl.execution.StreamEvaluator;

import java.util.Map;

@Tags({"name"})
public class AppDefinitionEntity extends TaggedLogAPIEntity {
    private long updateTimestamp;
    private long createTimestamp;
    /**
     * Definition code
     */
    private String definition;
    private Map<String,Object> environment;

    private String creator;
    private String description;

    public void validate() throws Exception {
        if(definition == null) throw new IllegalArgumentException("definition should not empty");
        new StreamEvaluator(definition, ConfigFactory.parseMap(environment)).compile();
    }
}