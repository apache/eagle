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
import com.typesafe.config.Config;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicCredentials;
import io.dropwizard.setup.Environment;
import org.apache.eagle.server.authentication.authenticator.LdapAuthenticator;
import org.apache.eagle.server.authentication.authenticator.SimpleAuthenticator;
import org.apache.eagle.server.authentication.principal.User;

public class AuthenticationModeIdentifier {
    private static final String SIMPLE_MODE_KEYWORD = "simple";
    private static final String SIMPLE_MODE_REALM = "SIMPLE_AUTHENTICATION";
    private static final String LDAP_MODE_KEYWORD = "ldap";
    private static final String LDAP_MODE_REALM = "LDAP_AUTHENTICATION";

    private static final String MODE_KEY = "auth.mode";
    private static final String ACCEPTED_USERNAME_KEY = "auth.basic.username";
    private static final String ACCEPTED_PASSWORD_KEY = "auth.basic.password";
    private static final String CACHE_POLICY_KEY = "auth.cachePolicy";
    private static final String AUTHORIZE_KEY = "auth.authorize";
    private static final String CACHE_KEY = "auth.cache";
    private static final String AUTH_PARAMETER_ANNOTATION_KEY = "auth.parameter.annotation";

    private Config config = null;
    private Environment environment = null;

    private AuthenticationModeIdentifier(Config config, Environment environment) {
        this.config = config;
        this.environment = environment;
    }

    static AuthenticationModeIdentifier initiate(Config config, Environment environment) {
        return new AuthenticationModeIdentifier(config, environment);
    }

    AuthenticationMode<User> identify() {
        String modeKeyword = getModeKeyword();
        if (SIMPLE_MODE_KEYWORD.equalsIgnoreCase(modeKeyword)) {
            return new AuthenticationMode<User>(this) {
                public Authenticator<BasicCredentials, User> createAuthenticator() {
                    return new SimpleAuthenticator(
                            getIdentifier().getConfig().getString(AuthenticationModeIdentifier.ACCEPTED_USERNAME_KEY),
                            getIdentifier().getConfig().getString(AuthenticationModeIdentifier.ACCEPTED_PASSWORD_KEY)
                    );
                }

                public String getRealm() {
                    return AuthenticationModeIdentifier.SIMPLE_MODE_REALM;
                }
            };
        }
        if (LDAP_MODE_KEYWORD.equalsIgnoreCase(modeKeyword)) {
            return new AuthenticationMode<User>(this) {
                public Authenticator<BasicCredentials, User> createAuthenticator() {
                    return new LdapAuthenticator(getIdentifier().getConfig());
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
        return config.getBoolean(CACHE_KEY);
    }

    boolean authorizationRequired() {
        return config.getBoolean(AUTHORIZE_KEY);
    }

    boolean parameterAnnotationEnabled() {
        return config.getBoolean(AUTH_PARAMETER_ANNOTATION_KEY);
    }

    CacheBuilderSpec getCacheBuilderSpec() {
        return CacheBuilderSpec.parse(config.getString(CACHE_POLICY_KEY));
    }

    private Config getConfig() {
        return config;
    }

    private Environment getEnvironment() {
        return environment;
    }

    private String getModeKeyword() {
        return config.getString(MODE_KEY);
    }
}
