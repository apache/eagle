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
package org.apache.eagle.server.authentication;

import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.CachingAuthenticator;
import io.dropwizard.auth.basic.BasicCredentials;

public abstract class AuthenticationMode<P> {
    private static final String PREFIX_VALUE = "Basic";

    protected AuthenticationModeIdentifier identifier = null;

    private Authenticator<BasicCredentials, P> authenticator = null;

    public AuthenticationMode(AuthenticationModeIdentifier identifier) {
        this.identifier = identifier;
        this.authenticator = createAuthenticator();
    }

    abstract Authenticator<BasicCredentials, P> createAuthenticator();

    abstract String getRealm();

    Authenticator<BasicCredentials, P> getAuthenticator() {
        return identifier.cacheRequired()? cache(authenticator): authenticator;
    }

    private Authenticator<BasicCredentials, P> cache(Authenticator<BasicCredentials, P> authenticator) {
        return new CachingAuthenticator<BasicCredentials, P>(identifier.getMetricRegistry(), authenticator, identifier.getCacheBuilderSpec());
    }

    AuthenticationModeIdentifier getIdentifier() {
        return identifier;
    }

}
