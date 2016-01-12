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

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.Tags;

@Tags({"name","id"})
public class AppExecutionEntity extends TaggedLogAPIEntity{
    private String status;
    private long updateTimestamp;

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setUpdateTimestamp(long updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public final static class STATUS{
        public final static String UNKNOWN = "UNKNOWN";
        public final static String INITIALIZED = "INITIALIZED";
        public final static String RUNNING = "RUNNING";
        public final static String STARTING = "STARTING";
        public final static String STOPPING = "STOPPING";
        public final static String STOPPED = "STOPPED";
    }

    public AppDefinitionEntity getAppDefinition(){
        return null;
    }
}