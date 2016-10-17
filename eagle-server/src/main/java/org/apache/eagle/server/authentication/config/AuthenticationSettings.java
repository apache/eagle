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

import io.dropwizard.Configuration;
import org.codehaus.jackson.annotate.JsonProperty;

public class AuthenticationSettings extends Configuration {
    private boolean enabled = false;
    private String mode = null;
    private boolean caching = false;
    private String cachePolicy = null;
    private boolean authorization = false;
    private boolean annotated = true;
    private SimpleSettings simple = new SimpleSettings();
    private LdapSettings ldap = new LdapSettings();

    @JsonProperty
    public boolean isEnabled() {
        return enabled;
    }

    @JsonProperty
    public AuthenticationSettings setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    @JsonProperty
    public String getMode() {
        return mode;
    }

    @JsonProperty
    public AuthenticationSettings setMode(String mode) {
        this.mode = mode;
        return this;
    }

    @JsonProperty
    public boolean needsCaching() {
        return caching;
    }

    @JsonProperty
    public AuthenticationSettings setCaching(boolean caching) {
        this.caching = caching;
        return this;
    }

    @JsonProperty
    public String getCachePolicy() {
        return cachePolicy;
    }

    @JsonProperty
    public AuthenticationSettings setCachePolicy(String cachePolicy) {
        this.cachePolicy = cachePolicy;
        return this;
    }

    @JsonProperty
    public boolean needsAuthorization() {
        return authorization;
    }

    @JsonProperty
    public AuthenticationSettings setAuthorization(boolean authorization) {
        this.authorization = authorization;
        return this;
    }

    @JsonProperty
    public boolean byAnnotated() {
        return annotated;
    }

    @JsonProperty
    public AuthenticationSettings setAnnotated(boolean annotated) {
        this.annotated = annotated;
        return this;
    }

    @JsonProperty("ldap")
    public LdapSettings getLdap() {
        return ldap;
    }

    @JsonProperty("ldap")
    public AuthenticationSettings setLdap(LdapSettings ldap) {
        this.ldap = ldap;
        return this;
    }

    @JsonProperty("simple")
    public SimpleSettings getSimple() {
        return simple;
    }

    @JsonProperty("simple")
    public AuthenticationSettings setSimple(SimpleSettings simple) {
        this.simple = simple;
        return this;
    }
}
