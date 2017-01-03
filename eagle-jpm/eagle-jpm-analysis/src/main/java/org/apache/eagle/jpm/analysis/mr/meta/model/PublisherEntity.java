/*
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

package org.apache.eagle.jpm.analysis.mr.meta.model;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.metadata.persistence.PersistenceEntity;

public class PublisherEntity extends PersistenceEntity {
    private String userId;
    private String mailAddress;

    public PublisherEntity(String userId, String mailAddress) {
        this.userId = userId;
        this.mailAddress = mailAddress;
    }

    @Override
    public String toString() {
        return String.format("PublisherEntity[userId=%s, mailAddress=%s]", userId, mailAddress);
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getMailAddress() {
        return mailAddress;
    }

    public void setMailAddress(String mailAddress) {
        this.mailAddress = mailAddress;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(userId)
                .append(mailAddress)
                .build();
    }

    @Override
    public boolean equals(Object that) {
        if (that == this) {
            return true;
        }

        if (!(that instanceof PublisherEntity)) {
            return false;
        }

        PublisherEntity another = (PublisherEntity)that;

        return another.userId.equals(this.userId) && another.mailAddress.equals(this.mailAddress);
    }
}
