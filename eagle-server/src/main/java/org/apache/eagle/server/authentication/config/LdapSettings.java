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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.server.authentication.config;

import org.codehaus.jackson.annotate.JsonProperty;

public class LdapSettings {
    private String uri = null;
    private String userFilter = null;
    private String groupFilter = null;
    private String userNameAttribute = null;
    private String groupNameAttribute = null;
    private String groupMembershipAttribute = null;
    private String groupClassName = null;
    private String[] restrictToGroups = null;
    private String connectTimeout = null;
    private String readTimeout = null;

    @JsonProperty
    public String getUri() {
        return uri;
    }

    @JsonProperty
    public LdapSettings setUri(String uri) {
        this.uri = uri;
        return this;
    }

    @JsonProperty
    public String getUserFilter() {
        return userFilter;
    }

    @JsonProperty
    public LdapSettings setUserFilter(String userFilter) {
        this.userFilter = userFilter;
        return this;
    }

    @JsonProperty
    public String getGroupFilter() {
        return groupFilter;
    }

    @JsonProperty
    public LdapSettings setGroupFilter(String groupFilter) {
        this.groupFilter = groupFilter;
        return this;
    }

    @JsonProperty
    public String getUserNameAttribute() {
        return userNameAttribute;
    }

    @JsonProperty
    public LdapSettings setUserNameAttribute(String userNameAttribute) {
        this.userNameAttribute = userNameAttribute;
        return this;
    }

    @JsonProperty
    public String getGroupNameAttribute() {
        return groupNameAttribute;
    }

    @JsonProperty
    public LdapSettings setGroupNameAttribute(String groupNameAttribute) {
        this.groupNameAttribute = groupNameAttribute;
        return this;
    }

    @JsonProperty
    public String getGroupMembershipAttribute() {
        return groupMembershipAttribute;
    }

    @JsonProperty
    public LdapSettings setGroupMembershipAttribute(String groupMembershipAttribute) {
        this.groupMembershipAttribute = groupMembershipAttribute;
        return this;
    }

    @JsonProperty
    public String getGroupClassName() {
        return groupClassName;
    }

    @JsonProperty
    public LdapSettings setGroupClassName(String groupClassName) {
        this.groupClassName = groupClassName;
        return this;
    }

    @JsonProperty
    public String[] getRestrictToGroups() {
        return restrictToGroups;
    }

    @JsonProperty
    public LdapSettings setRestrictToGroups(String[] restrictToGroups) {
        this.restrictToGroups = restrictToGroups;
        return this;
    }

    @JsonProperty
    public String getConnectTimeout() {
        return connectTimeout;
    }

    @JsonProperty
    public LdapSettings setConnectTimeout(String connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    @JsonProperty
    public String getReadTimeout() {
        return readTimeout;
    }

    @JsonProperty
    public LdapSettings setReadTimeout(String readTimeout) {
        this.readTimeout = readTimeout;
        return this;
    }
}
