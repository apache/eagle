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
package org.apache.eagle.metadata.persistence;

import org.apache.eagle.metadata.utils.UUIDGenerator;

import java.io.Serializable;

/**
 * Metadata Persistence Entity
 */
public abstract class PersistenceEntity implements Serializable{
    private String uuid;
    private long createdTime;
    private long modifiedTime;

    public String getUuid(){
        return this.uuid;
    }

    public void setUuid(String uuid){
        this.uuid = uuid;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(long modifiedTime) {
        this.modifiedTime = modifiedTime;
    }

    public void ensureDefault(){
        if(this.uuid == null || this.uuid.isEmpty()){
            this.uuid = UUIDGenerator.newUUID();
        }
        if(createdTime == 0){
            this.createdTime = System.currentTimeMillis();
        }
        this.modifiedTime = System.currentTimeMillis();
    }
}