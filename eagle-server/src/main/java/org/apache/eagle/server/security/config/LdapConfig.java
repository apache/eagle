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
package org.apache.eagle.server.security.config;

import io.dropwizard.util.Duration;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LdapConfig {

    private String providerUrl = "";
    private String strategy = "";
    private String principalTemplate = "";
    private String certificateAbsolutePath = "";
    private Duration connectingTimeout = Duration.parse("500ms");
    private Duration readingTimeout = Duration.parse("500ms");

    @JsonProperty
    public String getProviderUrl() {
        return providerUrl;
    }

    @JsonProperty
    public LdapConfig setProviderUrl(String providerUrl) {
        this.providerUrl = providerUrl;
        return this;
    }

    @JsonProperty
    public String getPrincipalTemplate() {
        return principalTemplate;
    }

    @JsonProperty
    public LdapConfig setPrincipalTemplate(String principalTemplate) {
        this.principalTemplate = principalTemplate;
        return this;
    }

    @JsonProperty
    public String getStrategy() {
        return strategy;
    }

    @JsonProperty
    public LdapConfig setStrategy(String strategy) {
        this.strategy = strategy;
        return this;
    }

    @JsonProperty
    public Duration getConnectingTimeout() {
        return connectingTimeout;
    }

    @JsonProperty
    public LdapConfig setConnectingTimeout(Duration connectingTimeout) {
        this.connectingTimeout = connectingTimeout;
        return this;
    }

    @JsonProperty
    public Duration getReadingTimeout() {
        return readingTimeout;
    }

    @JsonProperty
    public LdapConfig setReadingTimeout(Duration readingTimeout) {
        this.readingTimeout = readingTimeout;
        return this;
    }

    @JsonProperty
    public String getCertificateAbsolutePath() {
        return certificateAbsolutePath;
    }

    @JsonProperty
    public LdapConfig setCertificateAbsolutePath(String certificateAbsolutePath) {
        this.certificateAbsolutePath = certificateAbsolutePath;
        return this;
    }
}
