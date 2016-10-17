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

import com.google.common.cache.CacheBuilderSpec;
import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.model.Parameter;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.spi.inject.Injectable;
import io.dropwizard.auth.Auth;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.CachingAuthenticator;
import io.dropwizard.auth.basic.BasicAuthProvider;
import io.dropwizard.auth.basic.BasicCredentials;
import io.dropwizard.setup.Environment;
import org.apache.eagle.common.authentication.User;
import org.apache.eagle.server.authentication.authenticator.LdapBasicAuthenticator;
import org.apache.eagle.server.authentication.authenticator.SimpleBasicAuthenticator;
import org.apache.eagle.server.authentication.config.AuthenticationSettings;

import java.util.HashMap;
import java.util.Map;

public class BasicAuthProviderBuilder {
    private static final String SIMPLE_MODE_REALM = "SIMPLE_AUTHENTICATION";
    private static final String LDAP_MODE_REALM = "LDAP_AUTHENTICATION";
    private static final Map<String, BasicAuthProvider<User>> MAPPING = new HashMap<>();
    private AuthenticationSettings authSettings;
    private Environment environment;

    public BasicAuthProviderBuilder(AuthenticationSettings authSettings, Environment environment) {
        this.authSettings = authSettings;
        this.environment = environment;
        Authenticator<BasicCredentials, User> simpleAuthenticator = new SimpleBasicAuthenticator(authSettings.getSimple());
        Authenticator<BasicCredentials, User> ldapAuthenticator = new LdapBasicAuthenticator(authSettings.getLdap());
        boolean needsCaching = authSettings.needsCaching();
        MAPPING.put("simple",
                new BasicAuthProvider<>(needsCaching ? cache(simpleAuthenticator) : simpleAuthenticator, SIMPLE_MODE_REALM));
        MAPPING.put("ldap",
                new BasicAuthProvider<>(needsCaching ? cache(ldapAuthenticator) : ldapAuthenticator, LDAP_MODE_REALM));
    }

    public BasicAuthProvider build() {
        if (authSettings.isEnabled()) {
            String mode = authSettings.getMode();
            if (MAPPING.containsKey(mode)) {
                return MAPPING.get(mode);
            } else {
                throw new RuntimeException(String.format("No matching mode found: %s", mode));
            }
        } else {
            return new BasicAuthProvider<User>(null, "") {
                public Injectable<?> getInjectable(ComponentContext ic, Auth a, Parameter c) {
                    return new AbstractHttpContextInjectable<User>() {
                        public User getValue(HttpContext c) {
                            return new User("non-auth");
                        }
                    };
                }
            };
        }
    }

    private Authenticator<BasicCredentials, User> cache(Authenticator<BasicCredentials, User> authenticator) {
        return new CachingAuthenticator<>(environment.metrics(), authenticator, CacheBuilderSpec.parse(authSettings.getCachePolicy()));
    }
}
