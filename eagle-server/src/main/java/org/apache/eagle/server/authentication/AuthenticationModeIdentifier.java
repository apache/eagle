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

import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.CacheBuilderSpec;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicCredentials;
import io.dropwizard.setup.Environment;
import org.apache.eagle.common.authentication.User;
import org.apache.eagle.server.authentication.authenticator.LdapBasicAuthenticator;
import org.apache.eagle.server.authentication.authenticator.SimpleBasicAuthenticator;
import org.apache.eagle.server.authentication.config.AuthenticationSettings;

public class AuthenticationModeIdentifier {
    private static final String SIMPLE_MODE_KEYWORD = "simple";
    private static final String SIMPLE_MODE_REALM = "SIMPLE_AUTHENTICATION";
    private static final String LDAP_MODE_KEYWORD = "ldap";
    private static final String LDAP_MODE_REALM = "LDAP_AUTHENTICATION";

    private AuthenticationSettings settings = null;
    private Environment environment = null;

    private AuthenticationModeIdentifier(AuthenticationSettings settings, Environment environment) {
        this.settings = settings;
        this.environment = environment;
    }

    static AuthenticationModeIdentifier initiate(AuthenticationSettings config, Environment environment) {
        return new AuthenticationModeIdentifier(config, environment);
    }

    AuthenticationMode<User> identify() {
        String modeKeyword = getModeKeyword();
        if (SIMPLE_MODE_KEYWORD.equalsIgnoreCase(modeKeyword)) {
            return new AuthenticationMode<User>(this) {
                public Authenticator<BasicCredentials, User> createAuthenticator() {
                    return new SimpleBasicAuthenticator(getIdentifier().getSettings());
                }

                public String getRealm() {
                    return AuthenticationModeIdentifier.SIMPLE_MODE_REALM;
                }
            };
        }
        if (LDAP_MODE_KEYWORD.equalsIgnoreCase(modeKeyword)) {
            return new AuthenticationMode<User>(this) {
                public Authenticator<BasicCredentials, User> createAuthenticator() {
                    return new LdapBasicAuthenticator(getIdentifier().getSettings());
                }

                public String getRealm() {
                    return AuthenticationModeIdentifier.LDAP_MODE_REALM;
                }
            };
        }
        throw new RuntimeException(String.format("No matching mode can be found: %s", modeKeyword));
    }

    MetricRegistry getMetricRegistry() {
        return environment.metrics();
    }

    boolean cacheRequired() {
        return settings.needsCaching();
    }

    boolean authorizationRequired() {
        return settings.needsAuthorization();
    }

    boolean parameterAnnotationEnabled() {
        return settings.byAnnotated();
    }

    CacheBuilderSpec getCacheBuilderSpec() {
        return CacheBuilderSpec.parse(settings.getCachePolicy());
    }

    AuthenticationSettings getSettings() {
        return settings;
    }

    private Environment getEnvironment() {
        return environment;
    }

    private String getModeKeyword() {
        return settings.getMode();
    }
}
