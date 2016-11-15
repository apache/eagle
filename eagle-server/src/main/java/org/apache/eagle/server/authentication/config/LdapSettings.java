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

import io.dropwizard.util.Duration;
import org.codehaus.jackson.annotate.JsonProperty;

public class LdapSettings {

    private String providerUrl = "";
    private String strategy = "";
    private String principalTemplate = "";
    private Duration connectingTimeout = Duration.parse("500ms");
    private Duration readingTimeout = Duration.parse("500ms");

    @JsonProperty
    public String getProviderUrl() {
        return providerUrl;
    }

    @JsonProperty
    public LdapSettings setProviderUrl(String providerUrl) {
        this.providerUrl = providerUrl;
        return this;
    }

    @JsonProperty
    public String getPrincipalTemplate() {
        return principalTemplate;
    }

    @JsonProperty
    public LdapSettings setPrincipalTemplate(String principalTemplate) {
        this.principalTemplate = principalTemplate;
        return this;
    }

    @JsonProperty
    public String getStrategy() {
        return strategy;
    }

    @JsonProperty
    public LdapSettings setStrategy(String strategy) {
        this.strategy = strategy;
        return this;
    }

    @JsonProperty
    public Duration getConnectingTimeout() {
        return connectingTimeout;
    }

    @JsonProperty
    public LdapSettings setConnectingTimeout(Duration connectingTimeout) {
        this.connectingTimeout = connectingTimeout;
        return this;
    }

    @JsonProperty
    public Duration getReadingTimeout() {
        return readingTimeout;
    }

    @JsonProperty
    public LdapSettings setReadingTimeout(Duration readingTimeout) {
        this.readingTimeout = readingTimeout;
        return this;
    }
}
