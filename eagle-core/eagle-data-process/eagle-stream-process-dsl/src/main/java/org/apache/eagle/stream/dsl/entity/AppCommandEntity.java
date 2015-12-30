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

public class AppCommandEntity extends TaggedLogAPIEntity{
    private String type;
    private String status;

    private String targetId;
    private String targetName;

    private long createTimestamp;

    public long getExecuteTimestamp() {
        return executeTimestamp;
    }

    public void setExecuteTimestamp(long executeTimestamp) {
        this.executeTimestamp = executeTimestamp;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public void setCreateTimestamp(long createTimestamp) {
        this.createTimestamp = createTimestamp;
    }

    private long executeTimestamp;

    public final static class Type {
        public final static String START = "START";
        public final static String STOP = "STOP";
        public final static String RESTART = "RESTART";
    }
}